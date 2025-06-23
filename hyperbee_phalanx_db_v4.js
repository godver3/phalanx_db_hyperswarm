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
    
    this.swarm = new Hyperswarm({
      maxPeers: 128,
      maxParallel: 16,
      maxParallelPerPeer: 4,
      maxParallelPerPeer: 4,
    });
    this.store = new Corestore(this.storageDir);
    this.topic = crypto.createHash('sha256').update(this.topicString).digest();
    
    this.writer = null;
    this.view = null;
    this.knownWriters = new Set();
    this.indexing = new Map();
    this.indexingBee = null;
    this.rpc = null;
    this.rpcServer = null;
    this.isUpdating = false;
    
    // Batching configuration
    this.pendingOps = [];
    this.batchSize = options.batchSize || 100; // Group 100 operations per block
    this.batchTimeout = options.batchTimeout || 100; // Flush after 100ms
    this.batchTimer = null;
  }

  _debug(...args) {
    if (process.env.DEBUG === 'true') {
      const timestamp = new Date().toISOString();
      console.log(`[DEBUG ${timestamp}]`, ...args);
    }
  }

  async start() {
    this.writer = this.store.get({ name: 'local-writer' });
    this.view = new Hyperbee(this.store.get({ name: 'view' }), {
      keyEncoding: 'utf-8',
      valueEncoding: 'json'
    });
    this.indexingBee = new Hyperbee(this.store.get({ name: 'indexing-state' }), {
      keyEncoding: 'utf-8',
      valueEncoding: 'json'
    });

    await this.writer.ready();
    await this.view.ready();
    await this.indexingBee.ready();

    // Load persisted indexing state
    for await (const { key, value } of this.indexingBee.createReadStream()) {
      this.indexing.set(key, value);
    }

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
    this._updateView(); // Initial view update
  }

  async _flushBatch() {
    if (this.pendingOps.length === 0) return;
    
    const ops = this.pendingOps.splice(0, this.pendingOps.length);
    await this.writer.append(JSON.stringify({
      type: 'batch',
      ops: ops
    }));
    
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    
    this._updateView();
  }

  async _updateView() {
    if (this.isUpdating) {
      this._debug('[_updateView] Already updating, skipping.');
      return;
    }
    this._debug('[_updateView] Starting view update cycle.');
    this.isUpdating = true;

    try {
      for (const writerKeyHex of this.knownWriters) {
        const writerStartTime = Date.now();
        const core = this.store.get(Buffer.from(writerKeyHex, 'hex'));
        await core.ready();

        const startIndex = this.indexing.get(writerKeyHex) || 0;
        
        if (core.length <= startIndex) {
          continue;
        }

        this._debug(`[_updateView] Indexing writer ${writerKeyHex.slice(-6)} from ${startIndex} to ${core.length}. Total new ops: ${core.length - startIndex}`);
        
        this._debug(`[_updateView] Pre-fetching ${core.length - startIndex} blocks for writer ${writerKeyHex.slice(-6)}...`);
        const downloadStartTime = Date.now();
        const range = { start: startIndex, end: core.length };
        
        // Optimize download with parallel fetching
        const downloadOptions = {
          linear: false, // Allow non-linear downloading
          blocks: 1024,    // Download in chunks of 64 blocks
          parallel: 32    // Use 4 parallel download streams
        };
        
        await core.download(range, downloadOptions).done();
        const downloadTime = Date.now() - downloadStartTime;
        this._debug(`[_updateView] Pre-fetching complete in ${downloadTime}ms.`);

        const processStartTime = Date.now();
        const stream = core.createReadStream({ 
          start: startIndex, 
          end: core.length,
          highWaterMark: 64 // Read 64 blocks at a time into memory
        });
        let batch = this.view.batch();
        let blocksProcessed = 0;
        let opsInBatch = 0;
        let loopStartTime = Date.now();
        const LOG_INTERVAL = 10000; // Increased from 1000 to reduce logging overhead
        const BATCH_FLUSH_SIZE = 50000; // Increased from 10000 for better throughput

        for await (const block of stream) {
          blocksProcessed++;
          try {
            const data = JSON.parse(block);
            
            // Handle both single operations and batches
            if (data.type === 'batch' && Array.isArray(data.ops)) {
              // Process batched operations
              for (const op of data.ops) {
                if (op.type === 'put' || op.type === 'del') {
                  await batch[op.type](op.key, op.value);
                  opsInBatch++;
                }
              }
            } else if (data.type === 'put' || data.type === 'del') {
              // Process single operation (backward compatibility)
              await batch[data.type](data.key, data.value);
              opsInBatch++;
            }
          } catch (err) {
            console.error('Error processing block, skipping:', err);
          }

          if (opsInBatch > 0 && opsInBatch % LOG_INTERVAL === 0) {
            const now = Date.now();
            const duration = now - loopStartTime;
            const rate = (LOG_INTERVAL / (duration / 1000)).toFixed(2);
            this._debug(`[_updateView] Processed ${opsInBatch} ops in ${duration}ms (${rate} ops/s) for writer ${writerKeyHex.slice(-6)}.`);
            loopStartTime = now;
          }

          if (opsInBatch >= BATCH_FLUSH_SIZE) {
            this._debug(`[_updateView] Flushing batch of ${opsInBatch} for writer ${writerKeyHex.slice(-6)}.`);
            
            const flushStartTime = Date.now();
            await batch.flush();
            this._debug(`[_updateView] Main view flush took ${Date.now() - flushStartTime}ms.`);

            const indexFlushStartTime = Date.now();
            const newIndex = startIndex + blocksProcessed;
            this.indexing.set(writerKeyHex, newIndex);
            await this.indexingBee.put(writerKeyHex, newIndex);
            this._debug(`[_updateView] Index state flush took ${Date.now() - indexFlushStartTime}ms.`);
            
            batch = this.view.batch();
            opsInBatch = 0;
            loopStartTime = Date.now();
          }
        }

        if (opsInBatch > 0) {
          this._debug(`[_updateView] Flushing final batch of ${opsInBatch} for writer ${writerKeyHex.slice(-6)}.`);
          const flushStartTime = Date.now();
          await batch.flush();
          this._debug(`[_updateView] Final batch flush took ${Date.now() - flushStartTime}ms.`);
        }
        
        const finalIndex = core.length;
        this.indexing.set(writerKeyHex, finalIndex);
        await this.indexingBee.put(writerKeyHex, finalIndex);
        
        const processTime = Date.now() - processStartTime;
        const totalTime = Date.now() - writerStartTime;
        
        console.log(`Indexed writer ${writerKeyHex.slice(-6)} up to ${finalIndex}`);
        this._debug(`[_updateView] Writer ${writerKeyHex.slice(-6)} timing: download=${downloadTime}ms, process=${processTime}ms, total=${totalTime}ms`);
      }
    } catch (err) {
      console.error('Error during view update:', err);
    } finally {
      this.isUpdating = false;
      this._debug('[_updateView] Finished view update cycle.');
    }
  }

  async put(key, value) {
    this.pendingOps.push({
      type: 'put',
      key,
      value
    });
    
    if (this.pendingOps.length >= this.batchSize) {
      await this._flushBatch();
    } else if (!this.batchTimer) {
      this.batchTimer = setTimeout(() => this._flushBatch(), this.batchTimeout);
    }
  }

  async del(key) {
    this.pendingOps.push({
      type: 'del',
      key
    });
    
    if (this.pendingOps.length >= this.batchSize) {
      await this._flushBatch();
    } else if (!this.batchTimer) {
      this.batchTimer = setTimeout(() => this._flushBatch(), this.batchTimeout);
    }
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

  async bulkImport(entries) {
    console.log(`Starting bulk import of ${entries.length} entries...`);
    
    // Use much larger batches for bulk import
    const BULK_BATCH_SIZE = 1000;
    
    for (let i = 0; i < entries.length; i += BULK_BATCH_SIZE) {
      const batch = entries.slice(i, i + BULK_BATCH_SIZE);
      
      // Create a single large block with many operations
      await this.writer.append(JSON.stringify({
        type: 'batch',
        ops: batch.map(entry => ({
          type: 'put',
          key: entry.key,
          value: entry.value
        }))
      }));
      
      console.log(`Imported ${Math.min(i + BULK_BATCH_SIZE, entries.length)}/${entries.length} entries`);
    }
    
    // Force immediate view update after bulk import
    await this._updateView();
    console.log('Bulk import complete');
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

    const databaseEntries = Object.values(writerStats).reduce((sum, stats) => sum + stats.coreLength, 0);

    return {
      nodeId: this.writer.key.toString('hex'),
      topic: this.topicString,
      connectionsActive: this.swarm.connections.size,
      peersFound: this.swarm.peers.size,
      knownWriters: this.knownWriters.size,
      databaseEntries,
      writerStats,
      memory: process.memoryUsage()
    };
  }

  async stop() {
    // Flush any pending operations
    await this._flushBatch();
    
    if (this.rpcServer) await this.rpcServer.close();
    await this.swarm.destroy();
  }
}

export { P2PDBClient }; 