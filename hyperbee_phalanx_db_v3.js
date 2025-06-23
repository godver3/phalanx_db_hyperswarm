#!/usr/bin/env node
'use strict';

import path from 'path';
import crypto from 'crypto';
import Hyperswarm from 'hyperswarm';
import Corestore from 'corestore';
import Hyperbee from 'hyperbee';
import Autobase from 'autobase';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

// ES module equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

class AutobaseP2PClient {
  constructor(options = {}) {
    this.storageDir = options.storageDir || path.join(__dirname, 'autobase_storage');
    this.topicString = options.topic || 'hyperbee-phalanx-db-v3';
    this.swarm = new Hyperswarm();
    this.store = new Corestore(this.storageDir);
    this.autobase = null;
    this.topic = crypto.createHash('sha256').update(this.topicString).digest();
  }

  async start() {
    const writer = this.store.get({ name: 'writer' });
    await writer.ready();

    this.autobase = new Autobase(this.store, {
      apply: AutobaseP2PClient.apply,
      valueEncoding: 'json',
      input: writer,
      open: (store) => {
        // Autobase provides a namespaced store for the view
        return new Hyperbee(store.get({ name: 'view' }), {
            keyEncoding: 'utf-8',
            valueEncoding: 'json'
        });
      }
    });
    
    await this.autobase.ready();

    this.swarm.join(this.topic);
    await this.swarm.flush();

    console.log('Autobase P2P Client started.');
    console.log('Local writer key:', writer.key.toString('hex'));
    console.log('Autobase key:', this.autobase.key.toString('hex'));
    console.log('View type: Hyperbee');

    // Set up replication after autobase is ready
    this.swarm.on('connection', (socket, peerInfo) => {
      console.log('Peer connected, replicating...');
      this.store.replicate(socket);
    });

    console.log('Autobase initialized. Announcing writer automatically...');
    await this.announceWriter();

    // Less frequent updates to reduce load
    this.updateInterval = setInterval(() => {
      this.updateFromNetwork();
    }, 5000); // Every 5 seconds
  }

  async updateFromNetwork() {
    try {
      await this.autobase.update();
      
      // Check for new writers
      if (this.autobase.writers && this.autobase.writers.size > 0) {
        console.log('Current writers:', this.autobase.writers.size);
        if (this.autobase.writers.size > 1) {
          console.log('Writers:', Array.from(this.autobase.writers).map(w => w.toString('hex')));
        }
      }
    } catch (err) {
      console.error('Update error:', err.message);
    }
  }

  // Add method to manually announce writer after autobase is stable
  async announceWriter() {
    try {
      const writer = this.store.get({ name: 'writer' });
      
      // Ensure writer is ready before accessing its key
      await writer.ready();
      
      if (!writer.key) {
        throw new Error('Writer key is not available');
      }
      
      console.log('Manually announcing writer to network...');
      console.log('Writer key to announce:', writer.key.toString('hex'));
      
      await this.autobase.append({
        type: 'add-writer',
        key: writer.key.toString('hex')
      });
      console.log('Successfully announced writer to network');
      return true;
    } catch (err) {
      console.error('Error announcing writer:', err.message);
      console.error('Error details:', err.stack);
      return false;
    }
  }

  static async apply(batch, view, base) {
    if (!view || !batch || batch.length === 0) {
      return;
    }

    const b = view.batch({ update: false });

    for (const node of batch) {
      const op = node.value;

      if (op.type === 'add-writer') {
        try {
          const writerKey = Buffer.from(op.key, 'hex');
          await base.addWriter(writerKey, { indexer: true });
          console.log('Added new writer:', op.key);
        } catch (err) {
          console.error('Error adding writer:', err.message);
        }
        continue;
      }

      if (op.type === 'put' && op.key && op.value !== undefined) {
        await b.put(op.key, op.value);
      }
      if (op.type === 'del' && op.key) {
        await b.del(op.key);
      }
    }

    await b.flush();
  }

  async stop() {
    if (this.updateInterval) {
      clearInterval(this.updateInterval);
    }
    await this.swarm.destroy();
    await this.autobase.close();
  }

  get view() {
    return this.autobase.view;
  }

  async put(key, value) {
    await this.autobase.append({
      type: 'put',
      key,
      value
    });
  }

  async del(key) {
    await this.autobase.append({
      type: 'del',
      key
    });
  }

  async get(key) {
    // Ensure we have the latest data from the network
    await this.autobase.update();
    const result = await this.view.get(key);
    return result ? { key, value: result.value } : null;
  }

  async list() {
    // Ensure we have the latest data from the network  
    await this.autobase.update();
    const entries = [];
    for await (const { key, value } of this.view.createReadStream()) {
      entries.push({ key, value });
    }
    return entries;
  }
}

// Export the enhanced client
export { AutobaseP2PClient as P2PDBClient }; 