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
    this.lastSyncUpdate = null; // Track when last sync was updated
    this.activeEntriesCount = 0; // Track accurate entry count
    this.countInitialized = false; // Flag to track if initial count is done
    this.countCalculationPromise = null; // Track ongoing count calculation
    this.lastCountUpdate = null; // Track when count was last updated
    
    // Connection tracking for proper cleanup
    this.activeConnections = new Map(); // Track active peer connections
    this.pendingRpcRequests = new Map(); // Track pending RPC requests by peer
    
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

  _logMemoryUsage() {
    const memUsage = process.memoryUsage();
    const timestamp = new Date().toISOString();
    
    // Convert bytes to MB for readability
    const bytesToMB = (bytes) => {
      if (typeof bytes !== 'number' || isNaN(bytes)) return 0;
      return Math.round(bytes / 1024 / 1024 * 100) / 100;
    };
    
    // Calculate component-specific memory usage
    const componentMemory = {
      // Core Node.js memory
      rss: bytesToMB(memUsage.rss),
      heapTotal: bytesToMB(memUsage.heapTotal),
      heapUsed: bytesToMB(memUsage.heapUsed),
      external: bytesToMB(memUsage.external),
      arrayBuffers: bytesToMB(memUsage.arrayBuffers),
      
      // Component tracking
      connections: this.activeConnections.size,
      pendingRequests: this.pendingRpcRequests.size,
      knownWriters: this.knownWriters.size,
      indexingEntries: this.indexing.size,
      pendingOps: this.pendingOps.length,
      
      // Swarm stats
      swarmConnections: this.swarm.connections.size,
      swarmPeers: this.swarm.peers.size,
      
      // Database stats
      databaseEntries: this.countInitialized ? this.activeEntriesCount : 'Calculating...',
      countLastUpdate: this.lastCountUpdate ? new Date(this.lastCountUpdate).toISOString() : null,
      
      // Memory optimization status
      memoryOptimized: this.memoryOptimized,
      isUpdating: this.isUpdating
    };
    
    // Calculate heap fragmentation
    const heapFragmentation = memUsage.heapTotal > 0 ? 
      ((memUsage.heapTotal - memUsage.heapUsed) / memUsage.heapTotal * 100).toFixed(1) : 0;
    
    console.log(`[MEMORY ${timestamp}] RAM Usage Breakdown:`);
    console.log(`  RSS: ${componentMemory.rss}MB (Total process memory)`);
    console.log(`  Heap Total: ${componentMemory.heapTotal}MB (Allocated heap)`);
    console.log(`  Heap Used: ${componentMemory.heapUsed}MB (Active heap)`);
    console.log(`  Heap Fragmentation: ${heapFragmentation}%`);
    console.log(`  External: ${componentMemory.external}MB (C++ objects)`);
    console.log(`  Array Buffers: ${componentMemory.arrayBuffers}MB`);
    console.log(`  Active Connections: ${componentMemory.connections}`);
    console.log(`  Pending RPC Requests: ${componentMemory.pendingRequests}`);
    console.log(`  Known Writers: ${componentMemory.knownWriters}`);
    console.log(`  Indexing Entries: ${componentMemory.indexingEntries}`);
    console.log(`  Pending Operations: ${componentMemory.pendingOps}`);
    console.log(`  Swarm Connections: ${componentMemory.swarmConnections}`);
    console.log(`  Swarm Peers: ${componentMemory.swarmPeers}`);
    console.log(`  Database Entries: ${componentMemory.databaseEntries}`);
    console.log(`  Memory Optimized: ${componentMemory.memoryOptimized}`);
    console.log(`  View Updating: ${componentMemory.isUpdating}`);
    
    // Memory warnings
    const warnings = [];
    if (componentMemory.heapUsed > 500) warnings.push('High heap usage (>500MB)');
    if (componentMemory.external > 200) warnings.push('High external memory (>200MB)');
    if (componentMemory.connections > 50) warnings.push('Many active connections (>50)');
    if (componentMemory.pendingRequests > 10) warnings.push('Many pending RPC requests (>10)');
    if (componentMemory.knownWriters > 20) warnings.push('Many known writers (>20)');
    if (parseFloat(heapFragmentation) > 50) warnings.push('High heap fragmentation (>50%)');
    
    if (warnings.length > 0) {
      console.log(`  ⚠️  Warnings: ${warnings.join(', ')}`);
    }
    
    return componentMemory;
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
      const peerId = peerInfo.publicKey.toString('hex');
      const peerShortId = peerId.slice(-6);
      
      console.log(`Peer ${peerShortId} connected, replicating...`);
      this.store.replicate(socket);
      
      // Track this connection
      this.activeConnections.set(peerId, {
        socket,
        peerInfo,
        connectedAt: Date.now(),
        rpcAttempted: false
      });

      // Wait a bit to see if the connection is stable before making RPC request
      setTimeout(async () => {
        const connection = this.activeConnections.get(peerId);
        if (!connection) return; // Connection was already cleaned up
        
        // Check if socket is still active
        if (connection.socket && connection.socket.destroyed) {
          console.log(`Peer ${peerShortId} disconnected before RPC attempt`);
          return;
        }
        
        // Only make RPC request if we haven't already tried
        if (connection.rpcAttempted) return;
        connection.rpcAttempted = true;

        // Make RPC request with proper cleanup
        try {
          console.log(`Requesting writer key from peer ${peerShortId}`);
          
          // Track the pending request
          const requestPromise = this.rpc.request(peerInfo.publicKey, 'get_writer_key', null, { timeout: 5000 });
          this.pendingRpcRequests.set(peerId, requestPromise);
          
          const keyBuffer = await requestPromise;
          const newKey = keyBuffer.toString('utf-8');

          if (!this.knownWriters.has(newKey)) {
            this.knownWriters.add(newKey);
            console.log('Discovered new writer via RPC:', newKey);
            this._updateView();
          }
        } catch (err) {
          if (err.message.includes('CHANNEL_CLOSED')) {
            console.log(`RPC channel closed for peer ${peerShortId} (likely disconnected)`);
          } else {
            console.error(`RPC request to peer ${peerShortId} failed:`, err.message);
          }
        } finally {
          // Clean up the pending request
          this.pendingRpcRequests.delete(peerId);
        }
      }, 2000); // Wait 2 seconds before attempting RPC
    });

    // Handle peer disconnections
    this.swarm.on('disconnection', (socket, peerInfo) => {
      const peerId = peerInfo.publicKey.toString('hex');
      const peerShortId = peerId.slice(-6);
      
      console.log(`Peer ${peerShortId} disconnected`);
      
      // Clean up connection tracking
      const connection = this.activeConnections.get(peerId);
      if (connection) {
        const connectionDuration = Date.now() - connection.connectedAt;
        console.log(`Peer ${peerShortId} was connected for ${Math.round(connectionDuration / 1000)}s`);
        this.activeConnections.delete(peerId);
      }
      
      // Cancel any pending RPC requests for this peer
      const pendingRequest = this.pendingRpcRequests.get(peerId);
      if (pendingRequest) {
        // Note: We can't actually cancel the promise, but we can clean up the tracking
        this.pendingRpcRequests.delete(peerId);
        console.log(`Cleaned up pending RPC request for peer ${peerShortId}`);
      }
    });

    this.swarm.join(this.topic);
    await this.swarm.flush();

    console.log('P2P Client (V4) started. Topic:', this.topicString);
    
    // Start initial entry count calculation in background (non-blocking)
    this._calculateInitialCount().catch(err => {
      console.error('Background initial count calculation failed:', err);
    });
    
    // Periodically update the view from all known writers
    setInterval(() => this._updateView(), 5000);
    this._updateView(); // Initial view update
    
    // Periodically clean up stale connections
    setInterval(() => this.cleanupStaleConnections(), 120000); // Every 2 minutes - less aggressive
    
    // Periodically log detailed memory usage
    setInterval(() => this._logMemoryUsage(), 30000); // Every 30 seconds
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
    
    // Log memory before view update
    const memBefore = process.memoryUsage();
    const heapBefore = Math.round(memBefore.heapUsed / 1024 / 1024);
    this._debug(`[_updateView] Memory before update: ${heapBefore}MB heap used`);

    try {
      for (const writerKeyHex of this.knownWriters) {
        const writerStartTime = Date.now();
        const memUsage = process.memoryUsage();
        const initialMemory = (typeof memUsage.heapUsed === 'number' && !isNaN(memUsage.heapUsed)) 
          ? memUsage.heapUsed / 1024 / 1024 
          : 0;
        const core = this.store.get(Buffer.from(writerKeyHex, 'hex'));
        await core.ready();

        const startIndex = this.indexing.get(writerKeyHex) || 0;
        
        if (core.length <= startIndex) {
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

        for await (const block of stream) {
          blocksProcessed++;
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
        
        const finalIndex = core.length;
        this.indexing.set(writerKeyHex, finalIndex);
        await this.indexingBee.put(writerKeyHex, finalIndex);
        
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
        
        if (finalMemory > 0) {
          console.log(`Indexed writer ${writerKeyHex.slice(-6)} up to ${finalIndex}. Memory: ${Math.round(finalMemory)}MB (${memoryDelta >= 0 ? '+' : ''}${memoryDelta}MB)`);
        } else {
          console.log(`Indexed writer ${writerKeyHex.slice(-6)} up to ${finalIndex}`);
        }
        this._debug(`[_updateView] Writer ${writerKeyHex.slice(-6)} timing: download=${downloadTime}ms, process=${processTime}ms, total=${totalTime}ms`);
      }
    } catch (err) {
      console.error('Error during view update:', err);
    } finally {
      this.isUpdating = false;
      
      // Log memory after view update
      const memAfter = process.memoryUsage();
      const heapAfter = Math.round(memAfter.heapUsed / 1024 / 1024);
      const heapDelta = heapAfter - heapBefore;
      this._debug(`[_updateView] Memory after update: ${heapAfter}MB heap used (${heapDelta >= 0 ? '+' : ''}${heapDelta}MB)`);
      
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
    // If we have a cached timestamp and it's recent, use it
    if (this.lastSync && this.lastSyncUpdate) {
      const age = Date.now() - this.lastSyncUpdate;
      if (age < 300000) { // 5 minutes
        return this.lastSync;
      }
    }

    // If not cached or stale, find it by scanning the view in reverse (much faster)
    console.log('Last sync timestamp not cached or stale, scanning view in reverse to find it...');
    let mostRecent = null;
    let checked = 0;
    const MAX_CHECK = 1000; // Only check the last 1000 entries max
    
    try {
      // Use reverse stream to check the most recent entries first
      const stream = this.view.createReadStream({ 
        reverse: true,
        limit: MAX_CHECK,
        highWaterMark: this.streamHighWaterMark
      });
      
      for await (const { value } of stream) {
        checked++;
        if (value && value.last_modified) {
          mostRecent = value.last_modified;
          console.log(`Found recent timestamp in entry ${checked}: ${mostRecent}`);
          break; // Found one, that's the most recent since we're going in reverse
        }
        
        // Safety check - if we've checked too many without finding a timestamp, stop
        if (checked >= MAX_CHECK) {
          console.log(`Checked ${MAX_CHECK} most recent entries, no timestamp found`);
          break;
        }
      }
      
      this.lastSync = mostRecent;
      this.lastSyncUpdate = Date.now();
      console.log(`Discovered last sync timestamp: ${this.lastSync} (checked ${checked} entries)`);
      return this.lastSync;
    } catch (err) {
      console.error('Error finding latest sync timestamp:', err);
      return null;
    }
  }

  // Force refresh of last sync timestamp
  async refreshLatestSync() {
    console.log('Forcing refresh of last sync timestamp...');
    this.lastSync = null;
    this.lastSyncUpdate = null;
    return await this.getLatestSync();
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
    const databaseEntries = await this.getEntryCount();
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
      activeConnections: this.activeConnections.size,
      pendingRpcRequests: this.pendingRpcRequests.size,
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
    
    // Clean up all active connections and pending requests
    console.log(`Cleaning up ${this.activeConnections.size} active connections and ${this.pendingRpcRequests.size} pending RPC requests...`);
    
    // Clear all pending RPC requests
    this.pendingRpcRequests.clear();
    
    // Close all active connections
    for (const [peerId, connection] of this.activeConnections) {
      try {
        if (connection.socket && !connection.socket.destroyed) {
          connection.socket.destroy();
        }
      } catch (err) {
        console.log(`Error closing connection to peer ${peerId.slice(-6)}:`, err.message);
      }
    }
    this.activeConnections.clear();
    
    if (this.rpcServer) await this.rpcServer.close();
    await this.swarm.destroy();
    
    console.log('P2P Client stopped and cleaned up');
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

  // Clean up stale connections and requests
  cleanupStaleConnections() {
    const now = Date.now();
    const STALE_TIMEOUT = 300000; // 5 minutes - much more lenient for P2P
    const RPC_TIMEOUT = 60000; // 1 minute for RPC requests
    let cleanedConnections = 0;
    let cleanedRequests = 0;
    
    // Clean up stale connections (only if socket is actually closed)
    for (const [peerId, connection] of this.activeConnections) {
      const connectionAge = now - connection.connectedAt;
      
      // Only clean up if connection is very old AND socket is closed
      if (connectionAge > STALE_TIMEOUT && connection.socket && connection.socket.destroyed) {
        try {
          this.activeConnections.delete(peerId);
          cleanedConnections++;
        } catch (err) {
          console.log(`Error cleaning up stale connection to peer ${peerId.slice(-6)}:`, err.message);
        }
      }
    }
    
    // Clean up very old pending RPC requests
    for (const [peerId, requestPromise] of this.pendingRpcRequests) {
      // We can't easily track RPC request age, so just clean up if we have too many
      if (this.pendingRpcRequests.size > 50) {
        this.pendingRpcRequests.delete(peerId);
        cleanedRequests++;
      }
    }
    
    if (cleanedConnections > 0 || cleanedRequests > 0) {
      console.log(`Cleaned up ${cleanedConnections} stale connections and ${cleanedRequests} pending requests`);
    }
    
    return { cleanedConnections, cleanedRequests };
  }

  // Manually trigger memory logging
  logMemoryUsage() {
    return this._logMemoryUsage();
  }

  // Get detailed memory analysis
  getDetailedMemoryAnalysis() {
    const memUsage = process.memoryUsage();
    const componentMemory = this._logMemoryUsage();
    
    // Additional analysis
    const analysis = {
      timestamp: new Date().toISOString(),
      memoryUsage: componentMemory,
      recommendations: [],
      potentialIssues: []
    };
    
    // Memory pressure analysis
    if (componentMemory.heapUsed > 1000) {
      analysis.potentialIssues.push('Very high heap usage - consider garbage collection');
      analysis.recommendations.push('Call forceGC() to free memory');
    }
    
    if (componentMemory.external > 500) {
      analysis.potentialIssues.push('High external memory - possible memory leak in native code');
      analysis.recommendations.push('Check for unclosed streams or buffers');
    }
    
    if (componentMemory.connections > 100) {
      analysis.potentialIssues.push('Too many active connections');
      analysis.recommendations.push('Review connection cleanup logic');
    }
    
    if (componentMemory.pendingRequests > 20) {
      analysis.potentialIssues.push('Many pending RPC requests - possible network issues');
      analysis.recommendations.push('Check network connectivity and peer health');
    }
    
    // Performance recommendations
    if (!componentMemory.memoryOptimized && componentMemory.heapUsed > 200) {
      analysis.recommendations.push('Enable memory optimization for better performance');
    }
    
    if (componentMemory.knownWriters > 50) {
      analysis.recommendations.push('Consider reducing known writers or implementing writer cleanup');
    }
    
    return analysis;
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
    // Prevent multiple simultaneous calculations
    if (this.countCalculationPromise) {
      return this.countCalculationPromise;
    }
    
    this.countCalculationPromise = this._performCountCalculation();
    return this.countCalculationPromise;
  }

  async _performCountCalculation() {
    console.log('Calculating initial database entry count...');
    const startTime = Date.now();
    let count = 0;
    let processed = 0;
    const LOG_INTERVAL = 10000; // Log progress every 10k entries
    
    try {
      // Use a more efficient streaming approach with progress tracking
      const stream = this.view.createReadStream({
        highWaterMark: this.streamHighWaterMark
      });
      
      for await (const { key, value } of stream) {
        if (value) { // Only count non-null entries
          count++;
        }
        processed++;
        
        // Log progress for large databases
        if (processed % LOG_INTERVAL === 0) {
          const elapsed = Date.now() - startTime;
          const rate = (processed / (elapsed / 1000)).toFixed(2);
          console.log(`Count progress: ${processed} entries processed, ${count} valid entries found (${rate} entries/sec)`);
        }
      }
      
      this.activeEntriesCount = count;
      this.countInitialized = true;
      this.lastCountUpdate = Date.now();
      
      const totalTime = Date.now() - startTime;
      const finalRate = (processed / (totalTime / 1000)).toFixed(2);
      console.log(`Initial database entry count: ${count} (${processed} total entries processed in ${totalTime}ms, ${finalRate} entries/sec)`);
      
      return count;
    } catch (err) {
      console.error('Error calculating initial count:', err);
      this.activeEntriesCount = 0;
      this.countInitialized = true;
      this.lastCountUpdate = Date.now();
      return 0;
    } finally {
      this.countCalculationPromise = null;
    }
  }

  // Get current entry count (with fallback strategies)
  async getEntryCount() {
    // If we have a cached count and it's recent, use it
    if (this.countInitialized && this.lastCountUpdate) {
      const age = Date.now() - this.lastCountUpdate;
      if (age < 300000) { // 5 minutes
        return this.activeEntriesCount;
      }
    }
    
    // If count is not initialized, start calculation in background
    if (!this.countInitialized) {
      this._calculateInitialCount().catch(err => {
        console.error('Background count calculation failed:', err);
      });
      return 'Calculating...';
    }
    
    // For older counts, return cached value but trigger background refresh
    if (this.lastCountUpdate && (Date.now() - this.lastCountUpdate) > 300000) {
      // Trigger background refresh
      this._calculateInitialCount().catch(err => {
        console.error('Background count refresh failed:', err);
      });
    }
    
    return this.activeEntriesCount;
  }

  // Force a fresh count calculation
  async refreshEntryCount() {
    console.log('Forcing fresh entry count calculation...');
    this.countInitialized = false;
    this.countCalculationPromise = null;
    return await this._calculateInitialCount();
  }
}

export { P2PDBClient }; 