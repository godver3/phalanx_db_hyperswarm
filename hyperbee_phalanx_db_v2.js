#!/usr/bin/env node
'use strict';

import path from 'path';
import crypto from 'crypto';
import Hyperswarm from 'hyperswarm';
import Corestore from 'corestore';
import Hyperbee from 'hyperbee';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

// ES module equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Enhanced P2P Database with Vector Clocks and Delta Sync
class HyperbeeP2PDatabase {
  constructor(nodeId, options = {}) {
    this.nodeId = nodeId;
    this.storageDir = options.storageDir || path.join(__dirname, 'hyperbee_storage');
    this.debug = options.debug || false;
    this.preferNativeReplication = options.preferNativeReplication === true;
    this.seedHex = options.seedHex || null;
    this.writerKey = options.writerKey || null; // Key of the writer core to replicate from
    this.isWriter = options.isWriter !== false; // Default true for backward compatibility
    
    // Corestore and Hyperbee setup
    this.store = new Corestore(this.storageDir);
    this.bee = null;
    
    // Metadata tracking
    this.version = 0;
    this.lastModified = Date.now();
    this.activeEntriesCount = 0;
    
    // Vector clock for this node
    this.vectorClock = {};
    
    // Delta sync tracking
    this.syncStates = new Map(); // peerId -> lastSyncVersion
    
    // Metadata keys
    this.METADATA_PREFIX = '__metadata__';
    this.METADATA_VERSION_KEY = `${this.METADATA_PREFIX}:version`;
    this.METADATA_LAST_MODIFIED_KEY = `${this.METADATA_PREFIX}:lastModified`;
    this.METADATA_ACTIVE_COUNT_KEY = `${this.METADATA_PREFIX}:activeCount`;
    this.METADATA_VECTOR_CLOCK_KEY = `${this.METADATA_PREFIX}:vectorClock`;
    this.METADATA_SYNC_LOG_PREFIX = `${this.METADATA_PREFIX}:synclog:`;
  }

  async open() {
    let core;
    
    if (this.writerKey && !this.isWriter) {
      // Reader mode - replicate from writer's core
      core = this.store.get(Buffer.from(this.writerKey, 'hex'));
      await core.ready();
      
      if (this.debug) {
        console.log(`Reader core initialized - Key: ${core.key.toString('hex').slice(0, 8)}, Length: ${core.length}, Writable: ${core.writable}`);
      }
    } else {
      // Writer mode - create or get writable core
      core = this.seedHex ?
        this.store.get({ name: 'db', seed: Buffer.from(this.seedHex, 'hex') }) :
        this.store.get({ name: 'db' });
      
      await core.ready();
      
      if (this.debug) {
        console.log(`Writer core initialized - Key: ${core.key.toString('hex').slice(0, 8)}, Length: ${core.length}, Writable: ${core.writable}`);
        console.log(`Full writer key (share this with readers): ${core.key.toString('hex')}`);
      }
    }
    
    this.bee = new Hyperbee(core, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json'
    });
    await this.bee.ready();
    
    // Update Hyperbee to latest version if using native replication
    if (this.preferNativeReplication && !core.writable) {
      try {
        await this.bee.update();
        if (this.debug) {
          console.log(`Updated Hyperbee to latest version`);
        }
      } catch (err) {
        if (this.debug) {
          console.error('Error updating Hyperbee:', err);
        }
      }
    }
    
    // Load metadata including vector clock
    await this._loadMetadata();
    
    // Metadata loaded, database ready
    
    if (this.debug) {
      console.log(`Database opened. Version: ${this.version}, Entries: ${this.activeEntriesCount}`);
      console.log(`Vector clock:`, this.vectorClock);
    }

    // If we know we will rely on native replication to obtain data from peers
    // make sure we proactively download every block once replication starts.
    // Without this the local core stays sparse and databaseEntries count will
    // not grow until specific keys are accessed.
    if (this.preferNativeReplication) {
      // download all blocks (end:-1 means till current length and future appends)
      try {
        this.bee.core.download({ start: 0, end: -1 });
      } catch (_) {}
    }
  }

  async _loadMetadata() {
    try {
      // Debug core state
      if (this.debug) {
        console.log(`Core length: ${this.bee.core.length}, Writable: ${this.bee.core.writable}`);
      }
      
      // Load version
      const versionNode = await this.bee.get(this.METADATA_VERSION_KEY);
      if (versionNode?.value) {
        this.version = versionNode.value;
      }
      
      // Load last modified
      const lastModNode = await this.bee.get(this.METADATA_LAST_MODIFIED_KEY);
      if (lastModNode?.value) {
        this.lastModified = lastModNode.value;
      }
      
      // Load vector clock
      const vclockNode = await this.bee.get(this.METADATA_VECTOR_CLOCK_KEY);
      if (vclockNode?.value) {
        this.vectorClock = vclockNode.value;
      } else {
        this.vectorClock = { [this.nodeId]: 0 };
      }
      
      // Ensure this node has an entry in the vector clock.
      if (!this.vectorClock[this.nodeId]) {
        this.vectorClock[this.nodeId] = this.version; // Initialize with current version
        // Persist this change immediately
        if (this.bee.core.writable) {
          await this._updateMetadata(false);
        }
      }
      
      // Load or calculate active count
      const countNode = await this.bee.get(this.METADATA_ACTIVE_COUNT_KEY);
      if (countNode?.value) {
        this.activeEntriesCount = countNode.value;
      } else {
        this.activeEntriesCount = await this._calculateActiveEntries();
        // Only write the count if we're writable
        if (this.bee.core.writable) {
          await this.bee.put(this.METADATA_ACTIVE_COUNT_KEY, this.activeEntriesCount);
        }
      }
      
      // Check if we need to initialize metadata for existing entries
      if (this.activeEntriesCount > 0 && this.version === 0 && this.bee.core.writable) {
        console.log(`Detected ${this.activeEntriesCount} entries with no version metadata. Initializing...`);
        
        // Scan entries to build proper vector clock from existing data
        const stream = this.bee.createReadStream();
        const nodeVectorClocks = {};
        let maxVersion = 0;
        let entriesWithVectorClocks = 0;
        
        for await (const { key, value } of stream) {
          if (!key.startsWith(this.METADATA_PREFIX) && value) {
            // Aggregate vector clocks from all entries
            if (value.vectorClock) {
              entriesWithVectorClocks++;
              for (const [nodeId, clock] of Object.entries(value.vectorClock)) {
                nodeVectorClocks[nodeId] = Math.max(nodeVectorClocks[nodeId] || 0, clock);
              }
            }
            // Track version from sync logs if any
            if (value.version) {
              maxVersion = Math.max(maxVersion, value.version);
            }
          }
        }
        
        // Set vector clock based on existing data
        if (Object.keys(nodeVectorClocks).length > 0) {
          this.vectorClock = nodeVectorClocks;
          console.log(`Reconstructed vector clock from ${entriesWithVectorClocks} entries:`, this.vectorClock);
        } else {
          // No vector clocks in entries, initialize empty
          console.log(`No vector clocks found in entries.`);
        }
        
        // Ensure our node has an entry in the vector clock
        if (!this.vectorClock[this.nodeId]) {
          // Set our clock to the max of all clocks or activeEntriesCount
          const maxClock = Object.values(this.vectorClock).reduce((max, val) => Math.max(max, val), 0);
          this.vectorClock[this.nodeId] = Math.max(this.activeEntriesCount, maxClock);
          console.log(`Added local node to vector clock:`, this.vectorClock);
        }
        
        // Set version based on our node's vector clock value
        this.version = this.vectorClock[this.nodeId];
        
        console.log(`Initialized metadata: version=${this.version}, vectorClock=`, this.vectorClock);
        
        // Persist the initialized metadata
        await this._updateMetadata(false);
      }
    } catch (err) {
      console.error('Error loading metadata:', err);
    }
  }

  async _calculateActiveEntries() {
    let count = 0;
    const stream = this.bee.createReadStream();
    
    for await (const { key, value } of stream) {
      if (!key.startsWith(this.METADATA_PREFIX) && value && !value.deleted) {
        count++;
      }
    }
    
    return count;
  }

  _incrementVectorClock() {
    if (!this.vectorClock[this.nodeId] || this.vectorClock[this.nodeId] === 0) {
      // Initialize based on current state if not set
      const maxClock = Object.values(this.vectorClock).reduce((max, val) => Math.max(max, val), 0);
      this.vectorClock[this.nodeId] = Math.max(this.version, maxClock);
    }
    this.vectorClock[this.nodeId]++;
    return { ...this.vectorClock };
  }

  _compareVectorClocks(vc1, vc2) {
    // Returns:
    // -1: vc1 < vc2 (local is behind)
    //  0: vc1 === vc2 (equal)
    //  1: vc1 > vc2 (local is ahead)
    //  2: concurrent
    if (!vc1 || !vc2) return 2; // Treat as concurrent if one is missing

    let isLess = false;
    let isGreater = false;
    
    const allNodes = new Set([...Object.keys(vc1), ...Object.keys(vc2)]);
    
    for (const node of allNodes) {
      const v1 = vc1[node] || 0;
      const v2 = vc2[node] || 0;
      
      if (v1 < v2) isLess = true;
      if (v1 > v2) isGreater = true;
    }
    
    if (isLess && !isGreater) return -1; // vc1 is behind vc2
    if (isGreater && !isLess) return 1;  // vc1 is ahead of vc2
    if (!isLess && !isGreater) return 0; // equal
    return 2; // concurrent
  }

  _mergeVectorClocks(vc1, vc2) {
    const merged = { ...vc1 };
    for (const [node, version] of Object.entries(vc2)) {
      merged[node] = Math.max(merged[node] || 0, version);
    }
    return merged;
  }

  async _updateMetadata(incrementVersion = true) {
    // Only update metadata if we're writable
    if (!this.bee.core.writable) {
      return;
    }
    
    if (incrementVersion) {
      this.version++;
      this._incrementVectorClock();
    }
    this.lastModified = Date.now();
    
    const batch = this.bee.batch();
    await batch.put(this.METADATA_VERSION_KEY, this.version);
    await batch.put(this.METADATA_LAST_MODIFIED_KEY, this.lastModified);
    await batch.put(this.METADATA_ACTIVE_COUNT_KEY, this.activeEntriesCount);
    await batch.put(this.METADATA_VECTOR_CLOCK_KEY, this.vectorClock);
    await batch.flush();
  }

  async _logChange(key, operation, value) {
    // Only log changes if we're writable
    if (!this.bee.core.writable) {
      return;
    }
    
    // Log changes for delta sync
    const changeKey = `${this.METADATA_SYNC_LOG_PREFIX}${this.version}`;
    const change = {
      key,
      operation, // 'put' or 'delete'
      value,
      version: this.version,
      vectorClock: this.vectorClock,
      timestamp: Date.now()
    };
    await this.bee.put(changeKey, change);
  }

  async getChangesSince(version) {
    const changes = [];
    const stream = this.bee.createReadStream({
      gt: `${this.METADATA_SYNC_LOG_PREFIX}${version}`,
      lt: `${this.METADATA_SYNC_LOG_PREFIX}~`
    });
    
    for await (const { key, value } of stream) {
      changes.push(value);
    }
    
    return changes;
  }

  async cleanupOldSyncLogs(keepVersionsCount = 1000) {
    // Only cleanup if we're writable
    if (!this.bee.core.writable) {
      return 0;
    }
    
    const cutoffVersion = Math.max(0, this.version - keepVersionsCount);
    if (cutoffVersion === 0) return 0;
    
    let cleaned = 0;
    const batch = this.bee.batch();
    const stream = this.bee.createReadStream({
      gte: `${this.METADATA_SYNC_LOG_PREFIX}`,
      lt: `${this.METADATA_SYNC_LOG_PREFIX}${cutoffVersion}`
    });
    
    for await (const { key } of stream) {
      await batch.del(key);
      cleaned++;
    }
    
    if (cleaned > 0) {
      await batch.flush();
      if (this.debug) {
        console.log(`Cleaned up ${cleaned} old sync log entries`);
      }
    }
    
    return cleaned;
  }

  async addEntry(infohashService, cacheStatus, expiration) {
    try {
      // Check if we can write to this core
      if (!this.bee.core.writable) {
        console.error('Cannot add entry: Core is not writable');
        return false;
      }
      
      const existing = await this.bee.get(infohashService);
      const isNew = !existing?.value || existing.value.deleted;
      
      const entry = {
        cacheStatus,
        timestamp: new Date().toISOString(),
        expiration: expiration || new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(),
        updatedBy: this.nodeId,
        deleted: false,
        vectorClock: this._incrementVectorClock()
      };
      
      await this.bee.put(infohashService, entry);
      
      if (isNew) {
        this.activeEntriesCount++;
      }
      
      await this._updateMetadata();
      await this._logChange(infohashService, 'put', entry);
      
      return true;
    } catch (err) {
      console.error('Error adding entry:', err);
      return false;
    }
  }

  async updateEntry(infohashService, cacheStatus, expiration) {
    try {
      // Check if we can write to this core
      if (!this.bee.core.writable) {
        console.error('Cannot update entry: Core is not writable');
        return false;
      }
      
      const existing = await this.bee.get(infohashService);
      if (!existing?.value) {
        return false;
      }
      
      const wasDeleted = existing.value.deleted;
      
      const updatedEntry = {
        ...existing.value,
        cacheStatus: cacheStatus !== undefined ? cacheStatus : existing.value.cacheStatus,
        timestamp: new Date().toISOString(),
        expiration: expiration || existing.value.expiration,
        updatedBy: this.nodeId,
        deleted: false,
        vectorClock: this._incrementVectorClock()
      };
      
      await this.bee.put(infohashService, updatedEntry);
      
      if (wasDeleted) {
        this.activeEntriesCount++;
      }
      
      await this._updateMetadata();
      await this._logChange(infohashService, 'put', updatedEntry);
      
      return true;
    } catch (err) {
      console.error('Error updating entry:', err);
      return false;
    }
  }

  async deleteEntry(infohashService) {
    try {
      // Check if we can write to this core
      if (!this.bee.core.writable) {
        console.error('Cannot delete entry: Core is not writable');
        return false;
      }
      
      const existing = await this.bee.get(infohashService);
      if (!existing?.value) {
        return false;
      }
      
      const wasActive = !existing.value.deleted;
      
      const tombstone = {
        ...existing.value,
        deleted: true,
        timestamp: new Date().toISOString(),
        updatedBy: this.nodeId,
        vectorClock: this._incrementVectorClock()
      };
      
      await this.bee.put(infohashService, tombstone);
      
      if (wasActive) {
        this.activeEntriesCount--;
      }
      
      await this._updateMetadata();
      await this._logChange(infohashService, 'delete', tombstone);
      
      return true;
    } catch (err) {
      console.error('Error deleting entry:', err);
      return false;
    }
  }

  async get(key) {
    const node = await this.bee.get(key);
    return node?.value;
  }

  async _getAllEntries() {
    const entries = {};
    const stream = this.bee.createReadStream();
    
    for await (const { key, value } of stream) {
      if (!key.startsWith(this.METADATA_PREFIX)) {
        entries[key] = value;
      }
    }
    
    return entries;
  }

  async close() {
    if (this.bee) {
      await this.bee.close();
    }
    if (this.store) {
      await this.store.close();
    }
  }
}

// Enhanced P2P Client with Native Replication and Delta Sync
class HyperbeeP2PClient {
  constructor(options = {}) {
    if (!options.nodeId) throw new Error('P2PDBClient requires a nodeId');
    
    this.storageDir = options.storageDir || path.join(__dirname, 'hyperbee_storage_v2');
    this.nodeId = options.nodeId;
    this.topicString = options.topic || 'hyperbee-phalanx-db';
    this.debug = options.debug || false;
    this.preferNativeReplication = options.preferNativeReplication === true; // Default false - native replication disabled by default
    this.seedHex = options.seedHex || null;
    this.writerKey = options.writerKey || null;
    this.isWriter = options.isWriter !== false; // Default true for backward compatibility
    
    // Database instance
    this.db = null;
    
    // Networking
    this.swarm = null;
    this.topic = crypto.createHash('sha256').update(this.topicString).digest();
    this.peers = new Map();
    
    // Replication streams for native sync
    this.replicationStreams = new Map();
    
    // Stats
    this.stats = {
      syncsSent: 0,
      syncsReceived: 0,
      changesMerged: 0,
      deltaSyncs: 0,
      fullSyncs: 0,
      nativeReplications: 0,
      conflicts: 0,
      connectionsTotal: 0,
      errors: 0,
      reconnections: 0
    };
    
    // Connection retry configuration
    this.connectionRetries = new Map(); // peerId -> retryCount
    this.maxRetries = 3;
    this.retryDelay = 5000;
    
    // Cache for recent entries
    this.recentEntriesCache = null;
    this.cacheExpiry = 0;

    // Path for persisting sync state between restarts
    this.syncStateFile = path.join(this.storageDir, 'sync-states.json');
  }

  _initializeNodeId() {
    // This method is now obsolete. ID is managed by the server.
  }

  _saveNodeId() {
    // This method is now obsolete.
  }

  async initializeDatabase() {
    this.db = new HyperbeeP2PDatabase(this.nodeId, {
      storageDir: this.storageDir,
      debug: this.debug,
      preferNativeReplication: this.preferNativeReplication,
      seedHex: this.seedHex,
      writerKey: this.writerKey,
      isWriter: this.isWriter
    });
    await this.db.open();

    // After database is open, load any persisted per-peer sync versions
    this._loadSyncStates();
  }

  broadcast(message) {
    const msg = JSON.stringify({
      ...message,
      senderNodeId: this.nodeId,
      timestamp: Date.now()
    }) + '\n';
    
    for (const [peerId, socket] of this.peers) {
      if (!socket.destroyed && !socket._isReplicating) {
        socket.write(msg);
      }
    }
  }

  async handleMessage(message, fromPeer) {
    try {
      const socket = this.peers.get(fromPeer);
      if (!socket) return;

      if (message.type === 'hello') {
        // Enhanced hello with vector clock
        socket._peerVersion = message.version || 0;
        socket._peerEntryCount = message.entryCount || 0;
        socket._peerVectorClock = message.vectorClock || {};
        socket._peerVectorClockDigest = message.vectorClockDigest;
        socket._supportsVectorClocks = !!message.vectorClock;
        socket._supportsDeltaSync = !!message.supportsDeltaSync;
        socket._supportsNativeReplication = !!message.supportsNativeReplication;
        
        if (this.debug) {
          console.log(`Peer ${fromPeer} capabilities:`, {
            version: message.version,
            entries: message.entryCount,
            vectorClocks: socket._supportsVectorClocks,
            deltaSync: socket._supportsDeltaSync,
            nativeReplication: socket._supportsNativeReplication
          });
        }
        
        // Prefer native replication if both support it
        if (this.preferNativeReplication && socket._supportsNativeReplication && !socket._isLegacy) {
          // Switch to native replication
          socket._isReplicating = true;
          const stream = this.db.store.replicate(socket);
          this.replicationStreams.set(fromPeer, stream);
          this.stats.nativeReplications++;
          
          if (this.debug) {
            console.log(`Using native Hypercore replication with ${fromPeer}`);
          }
          
          // Trigger download of all blocks to ensure data syncs
          try {
            this.db.bee.core.download({ start: 0, end: -1 });
            if (this.debug) {
              console.log(`Triggered download for native replication with ${fromPeer}`);
            }
          } catch (err) {
            if (this.debug) {
              console.error('Error triggering download:', err);
            }
          }
          
          // Update local metadata after replication setup
          setTimeout(async () => {
            try {
              // Update Hyperbee to see latest changes
              await this.db.bee.update();
              if (this.debug) {
                console.log(`Updated Hyperbee after replication - Core length: ${this.db.bee.core.length}`);
              }
              
              await this.db._loadMetadata();
              if (this.debug) {
                console.log(`Reloaded metadata after native replication: Version: ${this.db.version}, Entries: ${this.db.activeEntriesCount}`);
              }
            } catch (err) {
              if (this.debug) {
                console.error('Error reloading metadata:', err);
              }
            }
          }, 2000);
          
          // Native replication will handle convergence; skip JSON-based sync logic
          return;
        }
        
        const clockComparison = this.db._compareVectorClocks(
          this.db.vectorClock,
          socket._peerVectorClock
        );
        
        // Fast path: digests match and clocks are identical, assume already in sync
        if (
          clockComparison === 0 &&
          socket._peerVectorClockDigest &&
          socket._peerVectorClockDigest === this._vectorClockDigest(this.db.vectorClock)
        ) {
          if (this.debug) console.log(`Peer ${fromPeer} already in sync (digest match)`);
          socket._skipSync = true;
          socket._syncInProgress = false; // Sync is over
          this._setSyncState(fromPeer, this.db.version);
          return; // Nothing else to do
        }

        switch (clockComparison) {
          case 0: // Equal
            if (this.debug) console.log(`Vector clocks are identical with ${fromPeer}. No sync needed.`);
            socket._skipSync = true;
            socket._syncInProgress = false; // Sync is officially over
            this._setSyncState(fromPeer, this.db.version);
            break;

          case -1: // We are behind
            if (this.debug) console.log(`Local is behind ${fromPeer}, requesting sync.`);
            if (!socket._syncInProgress) {
              socket._syncInProgress = true;
              this.requestSync(socket, fromPeer);
            }
            break;

          case 1: // We are ahead
            if (this.debug) console.log(`Local is ahead of ${fromPeer}, sending updates.`);
            if (!socket._syncInProgress) {
              socket._syncInProgress = true;
              this.sendSync(socket, fromPeer, message.lastSyncVersion);
            }
            break;

          case 2: // Concurrent
            if (this.debug) console.log(`Clocks are concurrent with ${fromPeer}. Exchanging updates.`);
            if (!socket._syncInProgress) {
              socket._syncInProgress = true;
              this.requestSync(socket, fromPeer);
              this.sendSync(socket, fromPeer, message.lastSyncVersion);
            }
            break;
        }
        
      } else if (message.type === 'put') {
        // Enhanced put with vector clock handling
        const existing = await this.db.get(message.key);
        
        if (!existing || !message.entry.vectorClock || !existing.vectorClock) {
          // Fallback to timestamp comparison
          if (!existing || existing.timestamp < message.entry.timestamp) {
            await this.db.bee.put(message.key, message.entry);
            this._updateLocalState(message.key, message.entry, existing);
          }
        } else {
          // Use vector clock comparison
          const comparison = this.db._compareVectorClocks(
            existing.vectorClock,
            message.entry.vectorClock
          );
          
          if (comparison === -1) {
            // Remote is newer
            await this.db.bee.put(message.key, message.entry);
            this._updateLocalState(message.key, message.entry, existing);
          } else if (comparison === 0) {
            // Same vector clock - check if data actually differs
            const existingData = { ...existing, timestamp: null, updatedBy: null };
            const remoteData = { ...message.entry, timestamp: null, updatedBy: null };
            
            if (JSON.stringify(existingData) !== JSON.stringify(remoteData)) {
              // Real concurrent update - resolve conflict
              this.stats.conflicts++;
              const resolved = this._resolveConflict(existing, message.entry);
              await this.db.bee.put(message.key, resolved);
              this._updateLocalState(message.key, resolved, existing);
            }
            // Otherwise, same data - no action needed
          }
          // If comparison === 1, local is newer, ignore remote
        }
        
      } else if (message.type === 'batch_put') {
        // Skip if we've determined sync isn't needed
        if (!socket._skipSync) {
          await this._handleBatchPut(message, fromPeer);
        } else if (this.debug) {
          console.log(`Skipping batch_put from ${fromPeer} - already in sync`);
        }
        
      } else if (message.type === 'delta_sync_request') {
        if (!socket._skipSync) {
          await this.sendDeltaSync(socket, message.sinceVersion);
        } else if (this.debug) {
          console.log(`Skipping delta_sync_request from ${fromPeer} - already in sync`);
        }
        
      } else if (message.type === 'delta_sync_response') {
        if (!socket._skipSync) {
          await this._handleDeltaSync(message, fromPeer);
        } else if (this.debug) {
          console.log(`Skipping delta_sync_response from ${fromPeer} - already in sync`);
        }
        
      } else if (message.type === 'sync_request') {
        if (!socket._skipSync && !socket._syncInProgress) {
          socket._syncInProgress = true;
          await this.sendFullStateOptimized(socket);
        } else if (this.debug) {
          console.log(`Skipping sync_request from ${fromPeer} - already in sync or sync in progress`);
        }
        
      } else if (message.type === 'sync_complete') {
        // Peer confirmed they're in sync
        socket._skipSync = true;
        this._setSyncState(fromPeer, message.version || this.db.version);
        if (this.debug) {
          console.log(`Peer ${fromPeer} confirmed sync complete`);
        }
      }
      
      this.stats.syncsReceived++;
    } catch (err) {
      console.error('Error handling message:', err);
      this.stats.errors++;
    }
  }

  requestSync(socket, fromPeer) {
    if (socket.destroyed) return;
    if (socket._supportsDeltaSync && this.db.syncStates.has(fromPeer)) {
      const lastSync = this.db.syncStates.get(fromPeer);
      socket.write(JSON.stringify({
        type: 'delta_sync_request',
        sinceVersion: lastSync,
        senderNodeId: this.nodeId,
        timestamp: Date.now()
      }) + '\n');
      if (this.debug) {
        console.log(`Requesting delta sync from ${fromPeer} since version ${lastSync}`);
      }
    } else {
      socket.write(JSON.stringify({
        type: 'sync_request',
        senderNodeId: this.nodeId,
        timestamp: Date.now()
      }) + '\n');
      if (this.debug) {
        console.log(`Requesting full sync from ${fromPeer}`);
      }
    }
  }

  async sendSync(socket, fromPeer, lastSyncVersion) {
    if (socket.destroyed) return;
    if (socket._supportsDeltaSync && lastSyncVersion !== undefined) {
      if (this.debug) {
        console.log(`Sending delta sync to ${fromPeer}.`);
      }
      await this.sendDeltaSync(socket, lastSyncVersion);
    } else {
      if (this.debug) {
        console.log(`Sending full sync to ${fromPeer}.`);
      }
      await this.sendFullStateOptimized(socket);
    }
  }

  _updateLocalState(key, entry, existing) {
    // Update vector clock
    this.db.vectorClock = this.db._mergeVectorClocks(
      this.db.vectorClock,
      entry.vectorClock || {}
    );
    
    // Invalidate cache
    this.recentEntriesCache = null;
    
    // Update count if needed
    if (!existing || existing.deleted !== entry.deleted) {
      if (entry.deleted && existing && !existing.deleted) {
        this.db.activeEntriesCount--;
      } else if (!entry.deleted && (!existing || existing.deleted)) {
        this.db.activeEntriesCount++;
      }
    }
    
    this.stats.changesMerged++;
  }

  _resolveConflict(local, remote) {
    // First check if this is a real conflict
    const localVC = JSON.stringify(local.vectorClock || {});
    const remoteVC = JSON.stringify(remote.vectorClock || {});
    const localData = { ...local, timestamp: null, updatedBy: null };
    const remoteData = { ...remote, timestamp: null, updatedBy: null };
    
    // If vector clocks and data are identical, prefer local (no real conflict)
    if (localVC === remoteVC && JSON.stringify(localData) === JSON.stringify(remoteData)) {
      if (this.debug) {
        console.log('False conflict detected (identical data), keeping local');
      }
      return local;
    }
    
    // Real conflict - merge fields, prefer higher cache status
    const resolved = {
      ...local,
      ...remote,
      cacheStatus: local.cacheStatus || remote.cacheStatus,
      timestamp: new Date().toISOString(),
      vectorClock: this.db._mergeVectorClocks(local.vectorClock, remote.vectorClock),
      updatedBy: this.nodeId // Use current node ID instead of concatenating
    };
    
    if (this.debug) {
      console.log('Resolved conflict:', { local, remote, resolved });
    }
    
    return resolved;
  }

  async _handleBatchPut(message, fromPeer) {
    if (!message.entries || !Array.isArray(message.entries)) return;
    
    const batch = this.db.bee.batch();
    let countChanges = { added: 0, deleted: 0 };
    let skippedIdentical = 0;
    
    for (const { key, value } of message.entries) {
      const existing = await this.db.get(key);
      
      let shouldApply = false;
      
      if (!existing || !value.vectorClock || !existing.vectorClock) {
        // Fallback to timestamp
        shouldApply = !existing || existing.timestamp < value.timestamp;
      } else {
        // Use vector clock
        const comparison = this.db._compareVectorClocks(
          existing.vectorClock,
          value.vectorClock
        );
        shouldApply = comparison === -1;
        
        if (comparison === 0) {
          // Same vector clock - check if data actually differs
          const existingData = { ...existing, timestamp: null, updatedBy: null };
          const remoteData = { ...value, timestamp: null, updatedBy: null };
          
          if (JSON.stringify(existingData) !== JSON.stringify(remoteData)) {
            // Real conflict - resolve and apply
            const resolved = this._resolveConflict(existing, value);
            await batch.put(key, resolved);
            this.stats.conflicts++;
          }
          // Otherwise same data - skip this entry
          continue;
        }
      }
      
      if (shouldApply) {
        await batch.put(key, value);
        
        // Track count changes
        if (!existing || existing.deleted !== value.deleted) {
          if (value.deleted && existing && !existing.deleted) {
            countChanges.deleted++;
          } else if (!value.deleted && (!existing || existing.deleted)) {
            countChanges.added++;
          }
        }
        
        this.stats.changesMerged++;
      } else {
        skippedIdentical++;
      }
    }
    
    // Apply all changes atomically
    await batch.flush();
    
    // Update counts and vector clock
    this.db.activeEntriesCount += countChanges.added - countChanges.deleted;
    
    // Merge the SENDER's vector clock, which is the most up-to-date representation.
    // This is the key to ensuring clocks converge.
    if (message.vectorClock) {
      this.db.vectorClock = this.db._mergeVectorClocks(
        this.db.vectorClock,
        message.vectorClock
      );
    }
    
    // If the sender included an up-to-date version, adopt it so our
    // subsequent hello messages reflect the true state and we don't get
    // bombarded with repeat full-syncs.
    if (message.upToVersion !== undefined) {
      this.db.version = Math.max(this.db.version, message.upToVersion);
    }
    
    await this.db._updateMetadata(false);
    
    // Update sync state
    if (message.upToVersion) {
      this._setSyncState(fromPeer, message.upToVersion);
    }
    
    // Invalidate cache
    this.recentEntriesCache = null;
    
    const applied = message.entries.length - skippedIdentical;
    
    // If all entries were skipped, we're already in sync
    if (applied === 0 && skippedIdentical > 0) {
      const socket = this.peers.get(fromPeer);
      if (socket) {
        socket._skipSync = true;
        if (this.debug) {
          console.log(`All ${skippedIdentical} entries from ${fromPeer} were identical - marking as synced`);
        }
      }
    } else if (applied > 0 || this.debug) {
      console.log(`Applied batch of ${applied} entries from ${fromPeer}${skippedIdentical > 0 ? ` (skipped ${skippedIdentical} identical)` : ''}`);
    }
  }

  async sendDeltaSync(socket, sinceVersion) {
    if (!socket || socket.destroyed) return;
    
    this.stats.deltaSyncs++;
    
    const changes = await this.db.getChangesSince(sinceVersion);
    
    if (this.debug) {
      console.log(`Sending delta sync with ${changes.length} changes since version ${sinceVersion}`);
    }
    
    // Group changes by key to only send latest
    const latestChanges = new Map();
    for (const change of changes) {
      latestChanges.set(change.key, change);
    }
    
    // Send as batch
    const entries = [];
    for (const change of latestChanges.values()) {
      if (change.operation === 'put') {
        entries.push({ key: change.key, value: change.value });
      }
    }
    
    const message = {
      type: 'delta_sync_response',
      entries,
      sinceVersion,
      upToVersion: this.db.version,
      vectorClock: this.db.vectorClock,
      senderNodeId: this.nodeId,
      timestamp: Date.now()
    };
    
    socket.write(JSON.stringify(message) + '\n');
  }

  async _handleDeltaSync(message, fromPeer) {
    await this._handleBatchPut(message, fromPeer);
    
    if (message.upToVersion !== undefined) {
      this._setSyncState(fromPeer, message.upToVersion);
    }
    
    if (this.debug) {
      console.log(`Delta sync from ${fromPeer} complete. ${message.entries.length} changes applied.`);
    }
  }

  async sendFullStateOptimized(socket) {
    if (!socket || socket.destroyed) return;
    
    this.stats.fullSyncs++;
    
    if (this.debug) {
      console.log('Starting optimized full state sync...', {
        reason: 'Peer missing data or vector clock mismatch',
        localVectorClock: this.db.vectorClock,
        peerVectorClock: socket._peerVectorClock,
        localVersion: this.db.version,
        peerVersion: socket._peerVersion
      });
    } else {
      console.log('Starting optimized full state sync...');
    }
    const BATCH_SIZE = 500;
    const MAX_MESSAGE_SIZE = 1024 * 1024; // 1MB per message
    
    let batch = [];
    let totalSent = 0;
    let messageSize = 0;
    
    const stream = this.db.bee.createReadStream();
    
    const sendBatch = async () => {
      if (batch.length === 0) return;
      
      const batchMessage = {
        type: 'batch_put',
        entries: batch,
        upToVersion: this.db.version,
        vectorClock: this.db.vectorClock,
        senderNodeId: this.nodeId,
        timestamp: Date.now()
      };
      
      socket.write(JSON.stringify(batchMessage) + '\n');
      
      totalSent += batch.length;
      if (totalSent % 1000 === 0) {
        console.log(`Sync progress: ${totalSent} entries sent`);
      }
      
      batch = [];
      messageSize = 0;
      
      if (totalSent > 10000) {
        await new Promise(resolve => setImmediate(resolve));
      }
    };
    
    for await (const { key, value } of stream) {
      if (!key.startsWith(this.db.METADATA_PREFIX)) {
        const entry = { key, value };
        const entrySize = JSON.stringify(entry).length;
        
        if (batch.length >= BATCH_SIZE || messageSize + entrySize > MAX_MESSAGE_SIZE) {
          await sendBatch();
          
          if (socket.destroyed) {
            console.log('Socket disconnected during sync');
            return;
          }
        }
        
        batch.push(entry);
        messageSize += entrySize;
      }
    }
    
    await sendBatch();
    
    const wasNecessary = totalSent > 0 || !socket._peerVectorClock;
    if (this.debug || !wasNecessary) {
      console.log(`Optimized sync complete. Sent ${totalSent} entries.${!wasNecessary ? ' (Sync may not have been necessary)' : ''}`);
    } else {
      console.log(`Optimized sync complete. Sent ${totalSent} entries.`);
    }

    // Mark peer as fully synced up to current version and avoid redundant syncs
    if (socket && socket._peerId) {
      this._setSyncState(socket._peerId, this.db.version);
    }
  }

  async start() {
    try {
      await this.initializeDatabase();
      
      this.swarm = new Hyperswarm();
      
      this.swarm.on('connection', (socket, info) => {
        const peerId = socket.remotePublicKey.toString('hex').slice(0, 8);
        
        // Deduplicate connections: if we already have a live socket to this peer
        if (this.peers.has(peerId) && !this.peers.get(peerId).destroyed) {
          if (this.debug) console.log(`Duplicate connection from ${peerId} – closing new socket`);
          socket.destroy();
          return;
        }
        socket._peerId = peerId;
        
        if (this.debug) {
          console.log('New peer connected:', peerId);
        }
        
        this.peers.set(peerId, socket);
        this.stats.connectionsTotal++;
        
        // Send enhanced hello
        socket.write(JSON.stringify({
          type: 'hello',
          senderNodeId: this.nodeId,
          version: this.db.version,
          entryCount: this.db.activeEntriesCount,
          vectorClock: this.db.vectorClock,
          vectorClockDigest: this._vectorClockDigest(this.db.vectorClock),
          supportsDeltaSync: true,
          supportsVectorClocks: true,
          supportsNativeReplication: true,
          lastSyncVersion: this.db.syncStates.get(peerId),
          timestamp: Date.now()
        }) + '\n');
        
        // Setup data handling
        let buffer = '';
        const MAX_BUFFER_SIZE = 10 * 1024 * 1024;
        let messagesProcessed = 0;

        socket._isLegacy = false;
        socket._isReplicating = false;

        socket.once('data', firstChunk => {
          // Check for native replication protocol
          if (firstChunk.length && firstChunk[0] < 0x20) {
            socket._isReplicating = true;
            const stream = this.db.store.replicate(socket);
            this.replicationStreams.set(peerId, stream);
            this.stats.nativeReplications++;
            
            if (this.debug) {
              console.log(`Peer ${peerId} using native Hypercore replication`);
            }
            
            // Trigger download of all blocks to ensure data syncs
            try {
              this.db.bee.core.download({ start: 0, end: -1 });
              if (this.debug) {
                console.log(`Triggered download for native replication with ${peerId}`);
              }
            } catch (err) {
              if (this.debug) {
                console.error('Error triggering download:', err);
              }
            }
            
            // Update local metadata after replication setup
            setTimeout(async () => {
              try {
                // Update Hyperbee to see latest changes
                await this.db.bee.update();
                if (this.debug) {
                  console.log(`Updated Hyperbee after replication - Core length: ${this.db.bee.core.length}`);
                }
                
                await this.db._loadMetadata();
                if (this.debug) {
                  console.log(`Reloaded metadata after native replication: Version: ${this.db.version}, Entries: ${this.db.activeEntriesCount}`);
                }
              } catch (err) {
                if (this.debug) {
                  console.error('Error reloading metadata:', err);
                }
              }
            }, 2000);
            
            return;
          }

          buffer += firstChunk.toString();
        });
        
        socket.on('data', data => {
          if (socket._isReplicating) return;

          buffer += data;
          
          if (buffer.length > MAX_BUFFER_SIZE) {
            if (this.debug) {
              console.error(`Buffer overflow from ${peerId}`);
            }
            buffer = '';
            return;
          }
          
          const lines = buffer.split('\n');
          buffer = lines.pop();
          
          for (const line of lines) {
            if (!line) continue;
            
            try {
              const message = JSON.parse(line);
              messagesProcessed++;
              
              // Detect legacy protocol
              const isLegacy = message.type === 'metadata_only' || 
                             message.type === 'sync_start' || 
                             message.type === 'sync_chunk' || 
                             message.type === 'sync_end' ||
                             message.syncChain !== undefined;
              
              if (isLegacy) {
                socket._isLegacy = true;
                
                if (message.type === 'metadata_only' && !socket._legacyHandled) {
                  socket._legacyHandled = true;
                  
                  const legacyResponse = {
                    type: 'metadata_only',
                    senderNodeId: this.nodeId,
                    version: this.db.version,
                    lastModified: this.db.lastModified,
                    contentHash: 0
                  };
                  socket.write(JSON.stringify(legacyResponse) + '\n');
                  
                  if (this.debug) {
                    console.log(`Sent legacy metadata to legacy peer ${peerId}`);
                  }
                }
                return;
              }
              
              // Process message
              setImmediate(() => {
                this.handleMessage(message, peerId).catch(err => {
                  if (this.debug) {
                    console.error('Message handling error:', err);
                  }
                });
              });
              
            } catch (err) {
              if (this.debug && !err.message.includes('Unexpected token')) {
                console.error('Parse error:', err.message);
              }
            }
          }
        });
        
        socket.on('close', () => {
          if (this.debug) {
            console.log('Peer disconnected:', peerId);
          }
          
          // Clean up replication stream
          const stream = this.replicationStreams.get(peerId);
          if (stream) {
            stream.destroy();
            this.replicationStreams.delete(peerId);
          }
          
          this.peers.delete(peerId);
        });
        
        socket.on('error', err => {
          if (this.debug) {
            console.error('Socket error:', err.message);
          }
          this.stats.errors++;
        });
      });
      
      await this.swarm.listen();
      this.swarm.join(this.topic, { server: true, client: true });
      
      console.log('Enhanced P2P network started');
      console.log('Node ID:', this.nodeId);
      console.log('Features: Native replication, Vector clocks, Delta sync');
      
      // Periodic delta sync with peers
      this.syncInterval = setInterval(() => {
        for (const [peerId, socket] of this.peers) {
          if (!socket.destroyed && !socket._isLegacy && !socket._isReplicating) {
            const lastSync = this.db.syncStates.get(peerId);
            if (lastSync && socket._supportsDeltaSync) {
              socket.write(JSON.stringify({
                type: 'delta_sync_request',
                sinceVersion: lastSync,
                senderNodeId: this.nodeId,
                timestamp: Date.now()
              }) + '\n');
            }
          }
        }
      }, 30000);
      
      // Periodic cleanup of old sync logs
      this.cleanupInterval = setInterval(async () => {
        try {
          const cleaned = await this.db.cleanupOldSyncLogs();
          if (cleaned > 0 && this.debug) {
            console.log(`Periodic cleanup: removed ${cleaned} old sync logs`);
          }
        } catch (err) {
          if (this.debug) {
            console.error('Error in periodic cleanup:', err);
          }
        }
      }, 300000); // Every 5 minutes
      
      // Periodic Hyperbee update when using native replication
      if (this.preferNativeReplication) {
        this.updateInterval = setInterval(async () => {
          try {
            const hasNativeReplications = this.replicationStreams.size > 0;
            if (hasNativeReplications) {
              const previousLength = this.db.bee.core.length;
              await this.db.bee.update();
              const newLength = this.db.bee.core.length;
              
              if (newLength > previousLength) {
                await this.db._loadMetadata();
                if (this.debug) {
                  console.log(`Periodic update: Core grew from ${previousLength} to ${newLength}, Entries: ${this.db.activeEntriesCount}`);
                }
              }
            }
          } catch (err) {
            if (this.debug) {
              console.error('Error in periodic update:', err);
            }
          }
        }, 5000);
      }
      
      return true;
    } catch (err) {
      console.error('Error starting P2P network:', err);
      return false;
    }
  }

  async stop() {
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
    }
    
    if (this.updateInterval) {
      clearInterval(this.updateInterval);
    }
    
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    
    // Clean up replication streams
    for (const [peerId, stream] of this.replicationStreams) {
      stream.destroy();
    }
    this.replicationStreams.clear();
    
    for (const [peerId, socket] of this.peers) {
      socket.destroy();
    }
    
    if (this.swarm) {
      await this.swarm.destroy();
    }
    
    if (this.db) {
      await this.db.close();
    }
    
    // Persist sync states one last time
    this._saveSyncStates();
    
    console.log('P2P network stopped');
    return true;
  }

  // API compatibility methods
  async addEntry(infohashService, cacheStatus, expiration) {
    const result = await this.db.addEntry(infohashService, cacheStatus, expiration);
    if (result) {
      this.recentEntriesCache = null;
      
      const entry = await this.db.get(infohashService);
      this.broadcast({
        type: 'put',
        key: infohashService,
        entry
      });
    }
    return result;
  }

  async updateEntry(infohashService, cacheStatus, expiration) {
    const result = await this.db.updateEntry(infohashService, cacheStatus, expiration);
    if (result) {
      this.recentEntriesCache = null;
      
      const entry = await this.db.get(infohashService);
      this.broadcast({
        type: 'put',
        key: infohashService,
        entry
      });
    }
    return result;
  }

  async deleteEntry(infohashService) {
    const result = await this.db.deleteEntry(infohashService);
    if (result) {
      this.recentEntriesCache = null;
      
      const entry = await this.db.get(infohashService);
      this.broadcast({
        type: 'put',
        key: infohashService,
        entry
      });
    }
    return result;
  }

  async getEntry(infohashService) {
    const entry = await this.db.get(infohashService);
    if (entry && !entry.deleted) {
      return entry;
    }
    return null;
  }

  async getAllEntries() {
    const allEntries = await this.db._getAllEntries();
    const activeEntries = {};
    
    for (const [key, entry] of Object.entries(allEntries)) {
      if (!entry.deleted) {
        activeEntries[key] = entry;
      }
    }
    
    return activeEntries;
  }

  async getStats() {
    const memoryUsage = process.memoryUsage();
    return {
      ...this.stats,
      connectionsActive: this.peers.size,
      replicationStreams: this.replicationStreams.size,
      databaseVersion: this.db.version,
      databaseEntries: this.db.activeEntriesCount,
      databaseLastModified: new Date(this.db.lastModified).toISOString(),
      vectorClock: this.db.vectorClock,
      nodeId: this.nodeId,
      memory: {
        heapTotal: Math.round(memoryUsage.heapTotal / 1024 / 1024) + ' MB',
        heapUsed: Math.round(memoryUsage.heapUsed / 1024 / 1024) + ' MB',
        rss: Math.round(memoryUsage.rss / 1024 / 1024) + ' MB',
        external: Math.round(memoryUsage.external / 1024 / 1024) + ' MB'
      }
    };
  }

  // Maintain getAllEntriesStructured and other API methods for compatibility
  async getAllEntriesStructured(options = {}) {
    const { limit = 100, startKey = undefined, sortByTime = true } = options;
    
    if (sortByTime) {
      const now = Date.now();
      let allEntries;
      
      if (this.recentEntriesCache && this.cacheExpiry > now) {
        allEntries = this.recentEntriesCache;
      } else {
        allEntries = [];
        const stream = this.db.bee.createReadStream();
        
        for await (const { key, value } of stream) {
          if (key.startsWith(this.db.METADATA_PREFIX) || value.deleted) {
            continue;
          }
          allEntries.push({ key, value });
        }
        
        allEntries.sort((a, b) => {
          const timeA = new Date(a.value.timestamp).getTime();
          const timeB = new Date(b.value.timestamp).getTime();
          return timeB - timeA;
        });
        
        this.recentEntriesCache = allEntries;
        this.cacheExpiry = now + 30000;
      }
      
      const startIndex = startKey ? parseInt(startKey) : 0;
      const endIndex = Math.min(startIndex + limit, allEntries.length);
      const pageEntries = allEntries.slice(startIndex, endIndex);
      
      const data = [];
      const infohashMap = new Map();
      
      for (const { key, value } of pageEntries) {
        const parts = key.split('+');
        if (parts.length < 2) continue;
        
        const infohash = parts[0];
        const service = parts.slice(1).join('+');
        
        let infohashEntry = infohashMap.get(infohash);
        if (!infohashEntry) {
          infohashEntry = {
            infohash,
            services: {}
          };
          data.push(infohashEntry);
          infohashMap.set(infohash, infohashEntry);
        }
        
        let cached = value.cacheStatus;
        if (typeof cached === 'string') {
          cached = cached === 'completed' || cached === 'cached';
        }
        
        infohashEntry.services[service] = {
          cached: cached,
          last_modified: value.timestamp,
          expiry: value.expiration
        };
      }
      
      return { 
        data,
        totalEntries: allEntries.length,
        nextKey: endIndex < allEntries.length ? endIndex.toString() : null
      };
    } else {
      // Key-based pagination
      const data = [];
      const infohashMap = new Map();
      let entriesProcessed = 0;
      
      const streamOptions = {};
      if (startKey) {
        streamOptions.gt = startKey;
      }
      
      const stream = this.db.bee.createReadStream(streamOptions);
      
      for await (const { key, value } of stream) {
        if (key.startsWith(this.db.METADATA_PREFIX) || value.deleted) {
          continue;
        }
        
        if (entriesProcessed >= limit) {
          break;
        }
        
        const parts = key.split('+');
        if (parts.length < 2) continue;
        
        const infohash = parts[0];
        const service = parts.slice(1).join('+');
        
        let infohashEntry = infohashMap.get(infohash);
        if (!infohashEntry) {
          infohashEntry = {
            infohash,
            services: {}
          };
          data.push(infohashEntry);
          infohashMap.set(infohash, infohashEntry);
        }
        
        let cached = value.cacheStatus;
        if (typeof cached === 'string') {
          cached = cached === 'completed' || cached === 'cached';
        }
        
        infohashEntry.services[service] = {
          cached: cached,
          last_modified: value.timestamp,
          expiry: value.expiration
        };
        
        entriesProcessed++;
      }
      
      return { data };
    }
  }

  transformFromStructuredFormat(structuredData) {
    const entries = {};
    
    if (structuredData.data && Array.isArray(structuredData.data)) {
      for (const item of structuredData.data) {
        const infohash = item.infohash;
        
        if (item.services) {
          for (const [service, serviceData] of Object.entries(item.services)) {
            const key = `${infohash}+${service}`;
            
            const isCached = typeof serviceData.cached === 'boolean' ? 
              serviceData.cached : 
              (serviceData.cached === 'true' || serviceData.cached === 'completed' || serviceData.cached === 'cached');
            
            const defaultExpiration = isCached ? 
              new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString() :
              new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
            
            entries[key] = {
              cacheStatus: isCached,
              timestamp: serviceData.last_modified || new Date().toISOString(),
              expiration: serviceData.expiry || defaultExpiration,
              updatedBy: this.nodeId
            };
          }
        }
      }
    }
    
    return entries;
  }

  async addEntriesStructured(structuredData) {
    const entries = this.transformFromStructuredFormat(structuredData);
    const results = [];
    
    for (const [key, entry] of Object.entries(entries)) {
      const result = await this.addEntry(key, entry.cacheStatus, entry.expiration);
      results.push({ key, success: result });
    }
    
    return results;
  }

  async updateEntriesStructured(structuredData) {
    const entries = this.transformFromStructuredFormat(structuredData);
    const results = [];
    
    for (const [key, entry] of Object.entries(entries)) {
      const result = await this.updateEntry(key, entry.cacheStatus, entry.expiration);
      results.push({ key, success: result });
    }
    
    return results;
  }

  // -------------------------
  // Sync-state persistence
  // -------------------------
  _loadSyncStates() {
    try {
      if (fs.existsSync(this.syncStateFile)) {
        const raw = fs.readFileSync(this.syncStateFile, 'utf8');
        const obj = JSON.parse(raw);
        for (const [peer, ver] of Object.entries(obj)) {
          this.db.syncStates.set(peer, ver);
        }
        if (this.debug) console.log(`Loaded syncStates for ${this.db.syncStates.size} peers from disk`);
      }
    } catch (err) {
      if (this.debug) console.error('Failed to load sync states:', err.message);
    }
  }

  _saveSyncStates() {
    try {
      const obj = {};
      for (const [peer, ver] of this.db.syncStates) obj[peer] = ver;
      fs.writeFileSync(this.syncStateFile, JSON.stringify(obj));
    } catch (err) {
      if (this.debug) console.error('Failed to save sync states:', err.message);
    }
  }

  _setSyncState(peerId, version) {
    if (version === undefined) return;
    this.db.syncStates.set(peerId, version);
    this._saveSyncStates();
  }

  // Deterministic digest of a vector clock to quickly detect equality
  _vectorClockDigest(vc) {
    const ordered = Object.keys(vc).sort().map(k => `${k}:${vc[k]}`).join('|');
    return crypto.createHash('sha256').update(ordered).digest('hex').slice(0, 16);
  }
}

// Export the enhanced client
export { HyperbeeP2PClient as P2PDBClient };

// Run as standalone if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const client = new HyperbeeP2PClient({ 
    debug: true,
    preferNativeReplication: true 
  });
  
  client.start()
    .then(async () => {
      console.log('Client started with enhanced features');
      
      // Show stats periodically
      setInterval(async () => {
        const stats = await client.getStats();
        console.log('\n=== Stats ===');
        console.log(`Native replications: ${stats.nativeReplications}`);
        console.log(`Delta syncs: ${stats.deltaSyncs}`);
        console.log(`Full syncs: ${stats.fullSyncs}`);
        console.log(`Conflicts resolved: ${stats.conflicts}`);
        console.log(`Vector clock:`, stats.vectorClock);
      }, 10000);
    })
    .catch(console.error);
  
  process.on('SIGINT', async () => {
    await client.stop();
    process.exit();
  });
} 