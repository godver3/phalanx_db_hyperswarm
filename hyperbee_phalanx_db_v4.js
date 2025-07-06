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
  /**
   * @param {Object} options - Configuration options
   * @param {string} options.storageDir - Directory for storing data
   * @param {string} options.topic - Topic string for P2P network
   * @param {number} options.batchSize - Number of operations to batch before writing (default: 100)
   * @param {number} options.batchTimeout - Time in ms to wait before flushing batch (default: 100)
   * @param {boolean} options.memoryOptimized - Enable memory optimization (default: true)
   * @param {number} options.maxBatchFlushSize - Max operations in view update batch (default: 5000 if optimized, 50000 if not)
   * @param {number} options.downloadParallel - Parallel download streams (default: 4 if optimized, 32 if not)
   * @param {number} options.downloadBlocks - Blocks per download chunk (default: 128 if optimized, 1024 if not)
   * @param {number} options.streamHighWaterMark - Stream buffer size (default: 16 if optimized, 64 if not)
   * @param {boolean} options.disablePreFetch - Disable pre-fetching of blocks (default: true if optimized)
   */
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
    this.lastSync = null; // To cache the last sync timestamp
    this.activeEntriesCount = 0; // Track accurate entry count
    this.countInitialized = false; // Flag to track if initial count is done
    
    // Memory management
    this.activeCores = new Map(); // Track active core references
    this.coreCleanupInterval = null;
    
    // Batching configuration - optimized for memory efficiency
    this.pendingOps = [];
    this.batchSize = options.batchSize || 100; // Group 100 operations per block
    this.batchTimeout = options.batchTimeout || 100; // Flush after 100ms
    this.batchTimer = null;
    
    // Memory optimization settings
    this.memoryOptimized = options.memoryOptimized !== false; // Default to true
    this.maxBatchFlushSize = options.maxBatchFlushSize || (this.memoryOptimized ? 5000 : 50000);
    this.downloadParallel = options.downloadParallel || (this.memoryOptimized ? 4 : 32);
    this.downloadBlocks = options.downloadBlocks || (this.memoryOptimized ? 128 : 1024);
    this.streamHighWaterMark = options.streamHighWaterMark || (this.memoryOptimized ? 16 : 64);
    this.disablePreFetch = options.disablePreFetch || this.memoryOptimized;
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
    
    // Calculate initial entry count
    await this._calculateInitialCount();
    
    // Periodically update the view from all known writers
    setInterval(() => this._updateView(), 5000);
    
    // Setup core cleanup to prevent memory leaks
    this.coreCleanupInterval = setInterval(() => this._cleanupUnusedCores(), 60000); // Every minute
    
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
        let finalIndex = null; // <-- ensure always defined
        const writerStartTime = Date.now();
        const memUsage = process.memoryUsage();
        const initialMemory = (typeof memUsage.heapUsed === 'number' && !isNaN(memUsage.heapUsed)) 
          ? memUsage.heapUsed / 1024 / 1024 
          : 0;
        this._debug(`[_updateView] Starting update for writer ${writerKeyHex.slice(-6)}. Initial heap: ${initialMemory}MB`);
        const core = this.store.get(Buffer.from(writerKeyHex, 'hex'));
        await core.ready();
        
        // Track this core for cleanup
        this.activeCores.set(writerKeyHex, {
          core,
          lastUsed: Date.now()
        });

        const startIndex = this.indexing.get(writerKeyHex) || 0;
        
        if (core.length <= startIndex) {
          this._debug(`[_updateView] No new ops for writer ${writerKeyHex.slice(-6)} (core.length=${core.length}, startIndex=${startIndex})`);
          continue;
        }

        const opsCount = core.length - startIndex;
        const memoryDisplay = initialMemory > 0 ? ` Memory: ${Math.round(initialMemory)}MB` : '';
        this._debug(`[_updateView] Indexing writer ${writerKeyHex.slice(-6)} from ${startIndex} to ${core.length}. Total new ops: ${opsCount}.${memoryDisplay}`);
        
        if (opsCount > 10000) {
          console.log(`Processing large batch: ${opsCount} operations for writer ${writerKeyHex.slice(-6)}.${memoryDisplay}`);
        }
        
        let downloadTime = 0;
        const processStartTime = Date.now();
        
        // Conditional pre-fetching based on memory optimization settings
        if (!this.disablePreFetch) {
          this._debug(`[_updateView] Pre-fetching ${core.length - startIndex} blocks for writer ${writerKeyHex.slice(-6)}...`);
          const downloadStartTime = Date.now();
          const range = { start: startIndex, end: core.length };
          
          // Use memory-optimized download settings
          const downloadOptions = {
            linear: false,
            blocks: this.downloadBlocks,
            parallel: this.downloadParallel
          };
          
          await core.download(range, downloadOptions).done();
          downloadTime = Date.now() - downloadStartTime;
          this._debug(`[_updateView] Pre-fetching complete in ${downloadTime}ms.`);
        } else {
          this._debug(`[_updateView] Skipping pre-fetch for memory optimization, processing ${core.length - startIndex} blocks for writer ${writerKeyHex.slice(-6)}...`);
        }

        const stream = core.createReadStream({ 
          start: startIndex, 
          end: core.length,
          highWaterMark: this.streamHighWaterMark
        });
        let batch = this.view.batch();
        let blocksProcessed = 0;
        let opsInBatch = 0;
        let loopStartTime = Date.now();
        const LOG_INTERVAL = 10000; // Increased from 1000 to reduce logging overhead
        const BATCH_FLUSH_SIZE = this.maxBatchFlushSize;
        
        // Track entry count changes during this update
        let entryCountDelta = 0;

        try {
          for await (const block of stream) {
          blocksProcessed++;
          if (blocksProcessed % 1000 === 0) {
            this._debug(`[_updateView] Processed ${blocksProcessed} blocks for writer ${writerKeyHex.slice(-6)}`);
          }
          try {
            const data = JSON.parse(block);
            
            // Handle both single operations and batches
            if (data.type === 'batch' && Array.isArray(data.ops)) {
              // Process batched operations
              for (const op of data.ops) {
                if (op.type === 'put') {
                  if (op.value && op.value.last_modified) {
                      if (!this.lastSync || new Date(op.value.last_modified) > new Date(this.lastSync)) {
                          this.lastSync = op.value.last_modified;
                      }
                  }
                  
                  // Track entry count changes for PUT operations
                  if (this.countInitialized) {
                    const existingEntry = await this.view.get(op.key);
                    if (!existingEntry || !existingEntry.value) {
                      entryCountDelta++; // New entry
                    }
                    // If entry exists, it's an update - no count change
                  }
                  
                  await batch.put(op.key, op.value);
                  opsInBatch++;
                } else if (op.type === 'del') {
                  // Track entry count changes for DELETE operations
                  if (this.countInitialized) {
                    const existingEntry = await this.view.get(op.key);
                    if (existingEntry && existingEntry.value) {
                      entryCountDelta--; // Deleted existing entry
                    }
                  }
                  
                  await batch.del(op.key);
                  opsInBatch++;
                }
              }
            } else if (data.type === 'put') {
              // Process single operation (backward compatibility)
              if (data.value && data.value.last_modified) {
                if (!this.lastSync || new Date(data.value.last_modified) > new Date(this.lastSync)) {
                    this.lastSync = data.value.last_modified;
                }
              }
              
              // Track entry count changes for single PUT operations
              if (this.countInitialized) {
                const existingEntry = await this.view.get(data.key);
                if (!existingEntry || !existingEntry.value) {
                  entryCountDelta++; // New entry
                }
              }
              
              await batch.put(data.key, data.value);
              opsInBatch++;
            } else if (data.type === 'del') {
                // Track entry count changes for single DELETE operations  
                if (this.countInitialized) {
                  const existingEntry = await this.view.get(data.key);
                  if (existingEntry && existingEntry.value) {
                    entryCountDelta--; // Deleted existing entry
                  }
                }
                
                await batch.del(data.key);
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
        
        finalIndex = core.length;
        this.indexing.set(writerKeyHex, finalIndex);
        await this.indexingBee.put(writerKeyHex, finalIndex);
        } finally {
          // Clean up resources to prevent memory leaks
          if (stream && typeof stream.destroy === 'function') {
            stream.destroy();
          }
          if (batch && typeof batch.flush === 'function') {
            try {
              await batch.flush();
            } catch (err) {
              this._debug(`[_updateView] Error flushing final batch: ${err.message}`);
            }
          }
        }
        
        // Apply entry count changes
        if (this.countInitialized && entryCountDelta !== 0) {
          this.activeEntriesCount += entryCountDelta;
          this._debug(`[_updateView] Entry count delta: ${entryCountDelta >= 0 ? '+' : ''}${entryCountDelta}, new total: ${this.activeEntriesCount}`);
        }
        
        const processTime = Date.now() - processStartTime;
        const totalTime = Date.now() - writerStartTime;
        const finalMemUsage = process.memoryUsage();
        const finalMemory = (typeof finalMemUsage.heapUsed === 'number' && !isNaN(finalMemUsage.heapUsed)) 
          ? finalMemUsage.heapUsed / 1024 / 1024 
          : 0;
        const memoryDelta = initialMemory > 0 && finalMemory > 0 ? Math.round(finalMemory - initialMemory) : 0;
        
        this._debug(`[_updateView] Writer ${writerKeyHex.slice(-6)} finished. Indexed up to ${finalIndex}. Memory: ${Math.round(finalMemory)}MB (${memoryDelta >= 0 ? '+' : ''}${memoryDelta}MB), processed ${blocksProcessed} blocks in ${totalTime}ms.`);
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
    
    // Use memory-optimized batch size for bulk import
    const BULK_BATCH_SIZE = this.memoryOptimized ? 500 : 1000;
    
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

  async getLatestSync() {
    if (this.lastSync) {
      return this.lastSync;
    }

    // If not cached, find it by scanning the view. This can be slow.
    console.log('Last sync timestamp not cached, scanning view to find it...');
    let mostRecent = null;
    for await (const { value } of this.view.createReadStream()) {
        if (value && value.last_modified) {
            if (!mostRecent || (new Date(value.last_modified) > new Date(mostRecent))) {
                mostRecent = value.last_modified;
            }
        }
    }
    this.lastSync = mostRecent;
    console.log(`Discovered last sync timestamp: ${this.lastSync}`);
    return this.lastSync;
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

    // Use accurate entry count instead of sum of core lengths
    const databaseEntries = this.countInitialized ? this.activeEntriesCount : 'Calculating...';
    const lastSync = await this.getLatestSync();
    const memoryUsage = process.memoryUsage();

    // Helper function to safely convert bytes to MB
    const bytesToMB = (bytes) => {
      if (typeof bytes !== 'number' || isNaN(bytes)) return '0 MB';
      return Math.round(bytes / 1024 / 1024) + ' MB';
    };

    return {
      nodeId: this.writer.key.toString('hex'),
      topic: this.topicString,
      connectionsActive: this.swarm.connections.size,
      peersFound: this.swarm.peers.size,
      knownWriters: this.knownWriters.size,
      databaseEntries,
      lastSync,
      writerStats,
      memory: {
        rss: bytesToMB(memoryUsage.rss),
        heapTotal: bytesToMB(memoryUsage.heapTotal),
        heapUsed: bytesToMB(memoryUsage.heapUsed),
        external: bytesToMB(memoryUsage.external),
        arrayBuffers: bytesToMB(memoryUsage.arrayBuffers)
      },
      config: {
        memoryOptimized: this.memoryOptimized,
        maxBatchFlushSize: this.maxBatchFlushSize,
        downloadParallel: this.downloadParallel,
        downloadBlocks: this.downloadBlocks,
        streamHighWaterMark: this.streamHighWaterMark,
        disablePreFetch: this.disablePreFetch
      }
    };
  }

  async stop() {
    // Flush any pending operations
    await this._flushBatch();
    
    // Clean up intervals
    if (this.coreCleanupInterval) {
      clearInterval(this.coreCleanupInterval);
      this.coreCleanupInterval = null;
    }
    
    // Clean up active cores
    this.activeCores.clear();
    
    if (this.rpcServer) await this.rpcServer.close();
    await this.swarm.destroy();
  }

  // Force garbage collection if available (useful after large operations)
  forceGC() {
    if (global.gc) {
      console.log('Forcing garbage collection...');
      global.gc();
      return true;
    } else {
      console.log('Garbage collection not available. Start Node.js with --expose-gc flag to enable.');
      return false;
    }
  }

  // Get memory optimization recommendations based on current usage
  getMemoryRecommendations() {
    const memoryUsage = process.memoryUsage();
    const heapUsedMB = (typeof memoryUsage.heapUsed === 'number' && !isNaN(memoryUsage.heapUsed)) 
      ? memoryUsage.heapUsed / 1024 / 1024 
      : 0;
    const recommendations = [];

    if (heapUsedMB > 500 && !this.memoryOptimized) {
      recommendations.push('Enable memory optimization: new P2PDBClient({ memoryOptimized: true })');
    }

    if (heapUsedMB > 1000) {
      recommendations.push('Consider reducing maxBatchFlushSize to 1000-2000');
      recommendations.push('Consider reducing downloadParallel to 2-4');
      recommendations.push('Consider enabling disablePreFetch: true');
    }

    if (this.knownWriters.size > 10) {
      recommendations.push('Large number of writers detected - memory usage scales with writer count');
    }

    if (recommendations.length === 0) {
      recommendations.push('Memory usage looks optimal');
    }

    return {
      currentHeapUsage: heapUsedMB > 0 ? Math.round(heapUsedMB) + ' MB' : 'Unknown',
      memoryOptimized: this.memoryOptimized,
      recommendations
    };
  }

  async _calculateInitialCount() {
    console.log('Calculating initial database entry count...');
    let count = 0;
    
    try {
      for await (const { key, value } of this.view.createReadStream()) {
        if (value) { // Only count non-null entries
          count++;
        }
      }
      
      this.activeEntriesCount = count;
      this.countInitialized = true;
      console.log(`Initial database entry count: ${count}`);
      return count;
    } catch (err) {
      console.error('Error calculating initial count:', err);
      this.activeEntriesCount = 0;
      this.countInitialized = true;
      return 0;
    }
  }

  async _cleanupUnusedCores() {
    const now = Date.now();
    const CORE_TIMEOUT = 5 * 60 * 1000; // 5 minutes
    
    for (const [writerKey, coreInfo] of this.activeCores.entries()) {
      if (now - coreInfo.lastUsed > CORE_TIMEOUT) {
        this._debug(`[_cleanupUnusedCores] Removing unused core for writer ${writerKey.slice(-6)}`);
        this.activeCores.delete(writerKey);
        
        // Force garbage collection if available
        if (global.gc) {
          global.gc();
        }
      }
    }
  }
}

export { P2PDBClient }; 