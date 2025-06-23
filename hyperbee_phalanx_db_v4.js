#!/usr/bin/env node
'use strict';

import path from 'path';
import crypto from 'crypto';
import Hyperswarm from 'hyperswarm';
import Corestore from 'corestore';
import Hyperbee from 'hyperbee';
import RPC from '@hyperswarm/rpc';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

class P2PDBClient {
  constructor(options = {}) {
    this.storageDir = options.storageDir || path.join(__dirname, 'autobase_storage_v4');
    this.topicString = options.topic || 'hyperbee-phalanx-db-v4';
    
    this.swarm = new Hyperswarm();
    this.store = new Corestore(this.storageDir);
    this.topic = crypto.createHash('sha256').update(this.topicString).digest();
    
    this.writer = null;
    this.view = null;
    this.knownWriters = new Set();
    this.indexing = new Map();
    this.rpc = null;
    this.rpcServer = null;
    this.isUpdating = false;
  }

  async start() {
    this.writer = this.store.get({ name: 'local-writer' });
    this.view = new Hyperbee(this.store.get({ name: 'view' }), {
      keyEncoding: 'utf-8',
      valueEncoding: 'json'
    });
    await this.writer.ready();
    await this.view.ready();

    this.knownWriters.add(this.writer.key.toString('hex'));
    console.log('Local writer key:', this.writer.key.toString('hex'));

    // Setup RPC first, before we start listening for connections.
    this.rpc = new RPC(this.swarm.dht);
    this.rpcServer = this.rpc.createServer();
    this.rpcServer.respond('get_writer_key', () => Buffer.from(this.writer.key.toString('hex'), 'utf-8'));
    await this.rpcServer.listen();
    console.log('RPC Server listening for writer key requests.');

    this.swarm.on('connection', async (socket, peerInfo) => {
      console.log('Peer connected, replicating...');
      this.store.replicate(socket);

      try {
        console.log(`Requesting writer key from peer ${peerInfo.publicKey.toString('hex').slice(-6)}`);
        const keyBuffer = await this.rpc.request(peerInfo.publicKey, 'get_writer_key', null, { timeout: 10000 });
        const newKey = keyBuffer.toString('utf-8');

        if (!this.knownWriters.has(newKey)) {
          this.knownWriters.add(newKey);
          console.log('Discovered new writer via RPC:', newKey);
          this._updateView();
        }
      } catch (err) {
        console.error(`RPC request to peer ${peerInfo.publicKey.toString('hex').slice(-6)} failed:`, err.message);
      }
    });

    this.swarm.join(this.topic);
    await this.swarm.flush();

    console.log('P2P Client (V4) started. Topic:', this.topicString);
    // Periodically update the view from all known writers
    setInterval(() => this._updateView(), 5000);
  }

  async _updateView() {
    if (this.isUpdating) return;
    this.isUpdating = true;

    try {
      for (const writerKeyHex of this.knownWriters) {
        const core = this.store.get(Buffer.from(writerKeyHex, 'hex'));
        await core.ready();

        const startIndex = this.indexing.get(writerKeyHex) || 0;
        
        if (core.length <= startIndex) continue;

        const stream = core.createReadStream({ start: startIndex, end: core.length });
        const b = this.view.batch();

        for await (const block of stream) {
          try {
            const op = JSON.parse(block);
            if (op.type === 'put' && op.key && op.value !== undefined) {
              await b.put(op.key, op.value);
            }
            if (op.type === 'del' && op.key) {
              await b.del(op.key);
            }
          } catch (err) {
            console.error('Error processing block:', err);
          }
        }
        await b.flush();
        this.indexing.set(writerKeyHex, core.length);
        console.log(`Indexed writer ${writerKeyHex.slice(-6)} up to ${core.length}`);
      }
    } catch (err) {
      console.error('Error during view update:', err);
    } finally {
      this.isUpdating = false;
    }
  }

  async put(key, value) {
    await this.writer.append(JSON.stringify({
      type: 'put',
      key,
      value
    }));
  }

  async del(key) {
    await this.writer.append(JSON.stringify({
      type: 'del',
      key
    }));
  }

  async get(key) {
    return this.view.get(key);
  }

  async list() {
    const entries = [];
    for await (const { key, value } of this.view.createReadStream()) {
      entries.push({ key, value });
    }
    return entries;
  }

  async getStats() {
    const writerStats = {};
    for (const writerKey of this.knownWriters) {
      const core = this.store.get(Buffer.from(writerKey, 'hex'));
      await core.ready();
      writerStats[writerKey.slice(-6)] = {
        key: writerKey,
        indexedLength: this.indexing.get(writerKey) || 0,
        coreLength: core.length
      };
    }

    let totalEntries = 0;
    for await (const _ of this.view.createReadStream()) {
      totalEntries++;
    }

    return {
      nodeId: this.writer.key.toString('hex'),
      topic: this.topicString,
      connectionsActive: this.swarm.connections.size,
      peersFound: this.swarm.peers.size,
      knownWriters: this.knownWriters.size,
      databaseEntries: totalEntries,
      viewStats: {
        version: this.view.version,
        byteLength: this.view.byteLength
      },
      writerStats,
      memory: process.memoryUsage()
    };
  }

  async stop() {
    if (this.rpcServer) await this.rpcServer.close();
    await this.swarm.destroy();
  }
}

export { P2PDBClient }; 