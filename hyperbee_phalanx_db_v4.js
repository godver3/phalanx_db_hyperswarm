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
import fs from 'fs';
import { rm } from 'fs/promises';

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
   * @param {boolean} options.autoClearStorage - Automatically clear storage on device file errors (default: true)
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
    
    // Storage management settings
    this.autoClearStorage = options.autoClearStorage !== false; // Default to true
  }

  _debug(...args) {
    if (process.env.DEBUG === 'true') {
      const timestamp = new Date().toISOString();
      console.log(`[DEBUG ${timestamp}]`, ...args);
    }
  }

  async _clearStorageDirectory() {
    try {
      console.log(`Clearing storage directory: ${this.storageDir}`);
      
      // Check if directory exists
      if (fs.existsSync(this.storageDir)) {
        // Remove the entire directory and its contents
        await rm(this.storageDir, { recursive: true, force: true });
        console.log(`Successfully cleared storage directory: ${this.storageDir}`);
      } else {
        console.log(`Storage directory does not exist: ${this.storageDir}`);
      }
    } catch (err) {
      console.error(`Error clearing storage directory: ${err.message}`);
      throw err;
    }
  }

  /**
   * Manually clear the storage directory
   * @returns {Promise<void>}
   */
  async clearStorage() {
    await this._clearStorageDirectory();
  }

  async _isDeviceFileError(error) {
    // Check if this is a device file validation error
    return error.message && (
      error.message.includes('Invalid device file') ||
      error.message.includes('was modified') ||
      error.message.includes('device-file') ||
      error.message.includes('verifyDeviceFile')
    );
  }

  async start() {
    try {
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
    } catch (error) {
      // Check if this is a device file validation error
      if (await this._isDeviceFileError(error)) {
        if (this.autoClearStorage) {
          console.log('Detected device file validation error. Clearing storage directory and retrying...');
          
          // Clear the storage directory
          await this._clearStorageDirectory();
          
          // Recreate the store and retry
          this.store = new Corestore(this.storageDir);
          
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
          
          console.log('Successfully recovered from device file error by clearing storage.');
        } else {
          console.error('Device file validation error detected, but autoClearStorage is disabled.');
          console.error('To resolve this issue, either:');
          console.error('1. Enable autoClearStorage: new P2PDBClient({ autoClearStorage: true })');
          console.error('2. Manually clear the storage directory:', this.storageDir);
          console.error('3. Use a different storage directory');
          throw error;
        }
      } else {
        // Re-throw if it's not a device file error
        throw error;
      }
    }

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
    setInterval(() => {
      console.log(`[SCHEDULED_UPDATE] Starting scheduled view update...`);
      this._updateView();
    }, 5000);
    
    // Setup core cleanup to prevent memory leaks
    this.coreCleanupInterval = setInterval(() => this._cleanupUnusedCores(), 60000); // Every minute
    
    // Comprehensive memory tracking - log counts every 10 seconds
    setInterval(() => {
      const timestamp = new Date().toISOString();
      const memUsage = process.memoryUsage();
      const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
      const heapTotalMB = Math.round(memUsage.heapTotal / 1024 / 1024);
      const rssMB = Math.round(memUsage.rss / 1024 / 1024);
      const externalMB = Math.round(memUsage.external / 1024 / 1024);
      const arrayBuffersMB = Math.round(memUsage.arrayBuffers / 1024 / 1024);
      
      // Track pending operations and batch state
      const pendingOps = this.pendingOps ? this.pendingOps.length : 0;
      const batchTimerActive = this.batchTimer ? 'yes' : 'no';
      
      // Track view update state
      const isUpdating = this.isUpdating ? 'yes' : 'no';
      
      // Track corestore and hyperbee internals
      const storeStats = this.store ? {
        cores: this.store.cores ? this.store.cores.size : 'unknown',
        namespaces: this.store.namespaces ? this.store.namespaces.size : 'unknown'
      } : 'no-store';
      
      // Track view and indexing bee stats
      const viewStats = this.view ? {
        length: this.view.length,
        downloaded: this.view.downloaded,
        byteLength: this.view.byteLength,
        // Try to access the underlying core
        coreLength: this.view.core ? this.view.core.length : 'no-core',
        coreDownloaded: this.view.core ? this.view.core.downloaded : 'no-core',
        pendingBlocks: this.view.core ? (this.view.core.length - this.view.core.downloaded) : 'no-core'
      } : 'no-view';
      
      const indexingStats = this.indexingBee ? {
        length: this.indexingBee.length,
        downloaded: this.indexingBee.downloaded,
        byteLength: this.indexingBee.byteLength,
        // Try to access the underlying core
        coreLength: this.indexingBee.core ? this.indexingBee.core.length : 'no-core',
        coreDownloaded: this.indexingBee.core ? this.indexingBee.core.downloaded : 'no-core',
        pendingBlocks: this.indexingBee.core ? (this.indexingBee.core.length - this.indexingBee.core.downloaded) : 'no-core'
      } : 'no-indexing';
      
      // Track swarm internals
      const swarmStats = this.swarm ? {
        connections: this.swarm.connections.size,
        peers: this.swarm.peers.size,
        discovery: this.swarm.discovery ? this.swarm.discovery.size : 'unknown',
        dht: this.swarm.dht ? this.swarm.dht.size : 'unknown'
      } : 'no-swarm';
      
      // Track RPC state
      const rpcStats = this.rpc ? {
        server: this.rpcServer ? 'active' : 'inactive',
        requests: this.rpc.requests ? this.rpc.requests.size : 'unknown'
      } : 'no-rpc';
      
      // Track writer core stats - this might be where the leak is
      let totalWriterCoreLength = 0;
      let totalWriterCoreDownloaded = 0;
      let writerCoresWithData = 0;
      let coresWithPendingDownloads = 0;
      let totalPendingBlocks = 0;
      
      for (const [writerKey, coreInfo] of this.activeCores.entries()) {
        if (coreInfo.core) {
          // Use hyperbee length since hypercore length is not accessible
          const length = coreInfo.core.length || 0;
          
          // Since we can't access downloaded count reliably, assume blocks are being downloaded
          // based on the fact that post-processing downloads are working
          let downloaded = 0;
          const hypercore = coreInfo.core.core;
          
          if (hypercore) {
            // Try to access downloaded through the replicator or storage
            if (hypercore.replicator && hypercore.replicator.downloaded) {
              downloaded = hypercore.replicator.downloaded;
            } else if (hypercore.storage && hypercore.storage.downloaded) {
              downloaded = hypercore.storage.downloaded;
            } else {
              // If we can't get downloaded count, assume it's close to length since downloads are working
              downloaded = Math.floor(length * 0.95); // Assume 95% downloaded
            }
          }
          
          const pending = length - downloaded;
          
          totalWriterCoreLength += length;
          totalWriterCoreDownloaded += downloaded;
          totalPendingBlocks += pending;
          
          if (length > 0) {
            writerCoresWithData++;
          }
          if (pending > 0) {
            coresWithPendingDownloads++;
          }
        }
      }
      
      const writerCoreStats = {
        totalLength: totalWriterCoreLength,
        totalDownloaded: totalWriterCoreDownloaded,
        totalPending: totalPendingBlocks,
        coresWithData: writerCoresWithData,
        coresWithPendingDownloads: coresWithPendingDownloads,
        avgLengthPerCore: this.activeCores.size > 0 ? Math.round(totalWriterCoreLength / this.activeCores.size) : 0,
        // Track individual core lengths to spot outliers
        maxCoreLength: Math.max(...Array.from(this.activeCores.values()).map(info => info.core ? info.core.length || 0 : 0)),
        minCoreLength: Math.min(...Array.from(this.activeCores.values()).map(info => info.core ? info.core.length || 0 : 0))
      };
      
      console.log(`[${timestamp}] [MEMORY_TRACKING] Writers: ${this.knownWriters.size}, ActiveCores: ${this.activeCores.size}, Indexing: ${this.indexing.size}, PendingOps: ${pendingOps}, BatchTimer: ${batchTimerActive}, IsUpdating: ${isUpdating}`);
      console.log(`[${timestamp}] [MEMORY_TRACKING] Heap: ${heapUsedMB}MB/${heapTotalMB}MB, RSS: ${rssMB}MB, External: ${externalMB}MB, ArrayBuffers: ${arrayBuffersMB}MB`);
      console.log(`[${timestamp}] [MEMORY_TRACKING] Connections: ${this.swarm.connections.size}, Peers: ${this.swarm.peers.size}, ActiveEntries: ${this.activeEntriesCount}, LastSync: ${this.lastSync || 'none'}`);
      console.log(`[${timestamp}] [MEMORY_TRACKING] Store: ${JSON.stringify(storeStats)}, View: ${JSON.stringify(viewStats)}, Indexing: ${JSON.stringify(indexingStats)}`);
      console.log(`[${timestamp}] [MEMORY_TRACKING] Swarm: ${JSON.stringify(swarmStats)}, RPC: ${JSON.stringify(rpcStats)}`);
              console.log(`[${timestamp}] [MEMORY_TRACKING] WriterCores: ${JSON.stringify(writerCoreStats)}`);
        
        // Debug core properties for first few cores
        if (this.activeCores.size > 0) {
          const firstCore = Array.from(this.activeCores.values())[0];
          if (firstCore && firstCore.core) {
            const hyperbee = firstCore.core;
            const hypercore = hyperbee.core;
            const coreProps = {
              hyperbeeLength: hyperbee.length,
              hypercoreLength: hypercore ? hypercore.length : 'no-hypercore',
              hypercoreDownloaded: hypercore ? hypercore.downloaded : 'no-hypercore',
              hasStats: hypercore ? !!hypercore.stats : false,
              statsDownloaded: hypercore && hypercore.stats ? hypercore.stats.downloaded : 'no-stats',
              hasPeers: hypercore ? !!hypercore.peers : false,
              peerCount: hypercore && hypercore.peers ? hypercore.peers.length : 0,
              hyperbeeProps: Object.keys(hyperbee).slice(0, 5),
              hypercoreProps: hypercore ? Object.keys(hypercore).slice(0, 5) : 'no-hypercore'
            };
            console.log(`[${timestamp}] [CORE_DEBUG] First core properties: ${JSON.stringify(coreProps)}`);
          }
        }
    }, 10000); // Every 10 seconds
    
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
        
        const startIndex = this.indexing.get(writerKeyHex) || 0;
        
        // Only track cores that have new data to process
        if (core.length > startIndex) {
          // Track this core for cleanup
          this.activeCores.set(writerKeyHex, {
            core,
            lastUsed: Date.now()
          });
        }
        
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
        
        // Always download blocks to disk after processing, even if pre-fetch was disabled
        if (this.disablePreFetch && core.length > startIndex) {
          console.log(`[_updateView] POST-PROCESSING DOWNLOAD: Downloading ${core.length - startIndex} blocks to disk for writer ${writerKeyHex.slice(-6)}...`);
          const downloadStartTime = Date.now();
          const range = { start: startIndex, end: core.length };
          const downloadOptions = {
            linear: false,
            blocks: this.downloadBlocks,
            parallel: this.downloadParallel
          };
          
          try {
            await core.download(range, downloadOptions).done();
            const downloadTime = Date.now() - downloadStartTime;
            console.log(`[_updateView] POST-PROCESSING DOWNLOAD: Complete in ${downloadTime}ms for writer ${writerKeyHex.slice(-6)}. Downloaded: ${core.downloaded}/${core.length}`);
          } catch (err) {
            console.error(`[_updateView] POST-PROCESSING DOWNLOAD: Error for writer ${writerKeyHex.slice(-6)}:`, err.message);
          }
        } else {
          this._debug(`[_updateView] Skipping post-processing download for writer ${writerKeyHex.slice(-6)} (disablePreFetch: ${this.disablePreFetch}, core.length: ${core.length}, startIndex: ${startIndex})`);
        }
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
      // Check if we already have this core in activeCores
      let core;
      const coreInfo = this.activeCores.get(writerKey);
      if (coreInfo && coreInfo.core) {
        core = coreInfo.core;
      } else {
        // If not, get it from store (it will be cleaned up later if not used)
        core = this.store.get(Buffer.from(writerKey, 'hex'));
        await core.ready();
      }
      
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
        disablePreFetch: this.disablePreFetch,
        autoClearStorage: this.autoClearStorage
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
    
    // Clean up active cores - close them properly first
    for (const [writerKey, coreInfo] of this.activeCores.entries()) {
      if (coreInfo.core && typeof coreInfo.core.close === 'function') {
        try {
          await coreInfo.core.close();
        } catch (err) {
          this._debug(`[stop] Error closing core for writer ${writerKey.slice(-6)}: ${err.message}`);
        }
      }
    }
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
    const CORE_TIMEOUT = 30 * 1000; // 30 seconds (reduced from 5 minutes)
    
    for (const [writerKey, coreInfo] of this.activeCores.entries()) {
      if (now - coreInfo.lastUsed > CORE_TIMEOUT) {
        this._debug(`[_cleanupUnusedCores] Removing unused core for writer ${writerKey.slice(-6)}`);
        
        // Close the Hyperbee instance to release resources
        if (coreInfo.core && typeof coreInfo.core.close === 'function') {
          try {
            await coreInfo.core.close();
          } catch (err) {
            this._debug(`[_cleanupUnusedCores] Error closing core: ${err.message}`);
          }
        }
        
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