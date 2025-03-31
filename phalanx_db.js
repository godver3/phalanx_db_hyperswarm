#!/usr/bin/env node
'use strict';

const Hyperswarm = require('hyperswarm');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const { Level } = require('level');

// Add logging setup
class Logger {
  constructor(options = {}) {
    this.debug = options.debug || false;
    this.logFile = options.logFile || path.join(__dirname, 'sync_debug.jsonl');
    this.console = options.console || false;
  }

  log(type, data) {
    const entry = {
      timestamp: new Date().toISOString(),
      type,
      ...data
    };

    // Write to file in JSONL format
    if (this.debug) {
      fs.appendFileSync(this.logFile, JSON.stringify(entry) + '\n');
    }

    // Optional console output
    if (this.console) {
      console.log(`[${entry.timestamp}] [${type}]`, data);
    }
  }
}

// Basic setup
const storageDir = path.join(__dirname, 'p2p-db-storage');
fs.mkdirSync(storageDir, { recursive: true });

// Function to load or save node ID
function getNodeId() {
  const nodeIdFile = path.join(storageDir, 'node-id.json');
  let id;
  
  if (fs.existsSync(nodeIdFile)) {
    try {
      id = JSON.parse(fs.readFileSync(nodeIdFile, 'utf8')).nodeId;
      console.log('Loaded existing node ID:', id);
    } catch (err) {
      id = crypto.randomBytes(4).toString('hex');
      fs.writeFileSync(nodeIdFile, JSON.stringify({ nodeId: id }));
      console.log('Generated new node ID:', id);
    }
  } else {
    id = crypto.randomBytes(4).toString('hex');
    fs.writeFileSync(nodeIdFile, JSON.stringify({ nodeId: id }));
    console.log('Generated new node ID:', id);
  }
  
  return id;
}

// Initialize nodeId
let nodeId = getNodeId();

// Database class for managing our data
class P2PDatabase {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this.dbDir = path.join(__dirname, 'db_data', nodeId);
    fs.mkdirSync(this.dbDir, { recursive: true });
    this.db = null;
    this.version = 0;
    this.lastModified = Date.now();
    
    // Define keys for storing metadata within LevelDB
    this.METADATA_VERSION_KEY = '__metadata_version__';
    this.METADATA_LAST_MODIFIED_KEY = '__metadata_lastModified__';
  }

  // Initialize the database
  async open() {
    if (this.db) {
      try {
        // Check if database is already open
        await this.db.get('__test__').catch(() => {});
        return; // Database is already open
      } catch (err) {
        if (err.code === 'LEVEL_DATABASE_NOT_OPEN') {
          // Try to close and reopen if not open
          try {
            await this.close();
          } catch (closeErr) {
            console.error('Error closing database:', closeErr);
          }
        } else {
          throw err;
        }
      }
    }

    let retries = 3;
    while (retries > 0) {
      try {
        this.db = new Level(this.dbDir, { valueEncoding: 'json' });
        await this.db.open();
        
        // *** NEW: Load metadata after opening ***
        try {
          const storedVersion = await this.db.get(this.METADATA_VERSION_KEY);
          this.version = storedVersion;
          console.log(`Loaded existing DB version: ${this.version}`);
        } catch (err) {
          if (err.notFound) {
            console.log('No existing DB version found, starting at 0.');
            // Optionally write initial metadata if not found
            await this.db.batch()
              .put(this.METADATA_VERSION_KEY, this.version)
              .put(this.METADATA_LAST_MODIFIED_KEY, this.lastModified)
              .write();
          } else {
            console.error('Error loading DB version:', err);
            throw err; // Re-throw other errors
          }
        }
        try {
           const storedLastModified = await this.db.get(this.METADATA_LAST_MODIFIED_KEY);
           this.lastModified = storedLastModified;
           console.log(`Loaded existing DB lastModified: ${new Date(this.lastModified).toISOString()}`);
        } catch (err) {
             if (!err.notFound) { // Ignore notFound, initial value is set above
               console.error('Error loading DB lastModified:', err);
               // Decide if this is fatal, maybe proceed with current timestamp
             }
        }
        // *** END NEW ***
        
        return; // Successfully opened and loaded metadata
      } catch (err) {
        retries--;
        if (err.code === 'LEVEL_LOCKED') {
          console.log(`Database is locked, retrying... (${retries} attempts left)`);
          await new Promise(resolve => setTimeout(resolve, 1000));
          continue;
        }
        throw err;
      }
    }
    throw new Error('Failed to open database after multiple attempts');
  }

  // Helper method to get all entries
  async _getAllEntries() {
    if (!this.db) {
      throw new Error('Database not initialized');
    }

    const entries = {};
    let iterator;
    try {
      iterator = this.db.iterator();
      for await (const [key, value] of iterator) {
        if (key !== '__test__') { // Skip test key
          entries[key] = value;
        }
      }
    } catch (err) {
      console.error('Error reading entries:', err);
    } finally {
      if (iterator) {
        await iterator.close().catch(err => {
          console.error('Error closing iterator:', err);
        });
      }
    }
    return entries;
  }

  // Helper to update metadata properties and persist to DB
  async _updateMetadata(batch) {
      const newVersion = this.version + 1;
      const newLastModified = Date.now();
      
      batch.put(this.METADATA_VERSION_KEY, newVersion);
      batch.put(this.METADATA_LAST_MODIFIED_KEY, newLastModified);
      
      // Update in-memory values *after* successful write (will happen post-batch.write())
      // We return the values to be set after the write succeeds.
      return { newVersion, newLastModified };
  }

  // Add an entry
  async addEntry(infohashService, cacheStatus, expiration) {
    if (!this.db) {
      throw new Error('Database not initialized');
    }

    try {
      const batch = this.db.batch();
      const newEntry = {
        cacheStatus,
        timestamp: new Date().toISOString(),
        expiration: expiration || new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(),
        updatedBy: this.nodeId,
        deleted: false
      };
      batch.put(infohashService, newEntry);
      
      // Add metadata updates to the batch
      const { newVersion, newLastModified } = await this._updateMetadata(batch);

      console.log(`[addEntry] Attempting to write version ${newVersion} for key ${infohashService}`);
      await batch.write(); // Write entry and metadata atomically
      console.log(`[addEntry] Successfully wrote batch for key ${infohashService}`);
      
      this.version = newVersion;
      this.lastModified = newLastModified;

      return true;
    } catch (err) {
      console.error(`[addEntry] Error writing batch for key ${infohashService}:`, err);
      return false;
    }
  }

  // Update an entry
  async updateEntry(infohashService, cacheStatus, expiration) {
    if (!this.db) {
      throw new Error('Database not initialized');
    }

    try {
      const existingEntry = await this.db.get(infohashService).catch(() => null);
      if (!existingEntry || existingEntry.deleted) { // Also check if marked deleted
        return false; // Don't update if non-existent or already deleted
      }
      
      const batch = this.db.batch();
      const updatedEntry = {
        ...existingEntry,
        cacheStatus: cacheStatus !== undefined ? cacheStatus : existingEntry.cacheStatus, // Allow updating only expiration
        timestamp: new Date().toISOString(),
        expiration: expiration || existingEntry.expiration,
        updatedBy: this.nodeId,
        deleted: false // Ensure deleted flag is false on update
      };
      batch.put(infohashService, updatedEntry);

      // Add metadata updates to the batch
      const { newVersion, newLastModified } = await this._updateMetadata(batch);
      
      console.log(`[updateEntry] Attempting to write version ${newVersion} for key ${infohashService}`);
      await batch.write(); // Write update and metadata atomically
      console.log(`[updateEntry] Successfully wrote batch for key ${infohashService}`);

      this.version = newVersion;
      this.lastModified = newLastModified;

      return true;
    } catch (err) {
      console.error(`[updateEntry] Error writing batch for key ${infohashService}:`, err);
      return false;
    }
  }

  // Delete an entry
  async deleteEntry(infohashService) {
    if (!this.db) {
      throw new Error('Database not initialized');
    }

    try {
      const existingEntry = await this.db.get(infohashService).catch(() => null);
      if (!existingEntry) {
        return false;
      }

      const batch = this.db.batch();
      const tombstoneEntry = {
        ...existingEntry,
        deleted: true,
        timestamp: new Date().toISOString(),
        updatedBy: this.nodeId
      };
      batch.put(infohashService, tombstoneEntry); // Overwrite with tombstone

      // Add metadata updates to the batch
      const { newVersion, newLastModified } = await this._updateMetadata(batch);
      
      console.log(`[deleteEntry] Attempting to write version ${newVersion} for key ${infohashService}`);
      await batch.write(); // Write tombstone and metadata atomically
      console.log(`[deleteEntry] Successfully wrote batch for key ${infohashService}`);
      
      this.version = newVersion;
      this.lastModified = newLastModified;
      
      return true;
    } catch (err) {
      console.error(`[deleteEntry] Error writing batch for key ${infohashService}:`, err);
      return false;
    }
  }

  // Merge with another database
  async merge(otherDb) {
    if (!this.db) {
      throw new Error('Database not initialized');
    }

    let changes = 0;
    let requiresMetadataUpdate = false; // Flag to track if metadata needs saving

    try {
        // Compare versions AND lastModified for more robust change detection
        if (otherDb.version <= this.version && otherDb.lastModified <= this.lastModified) {
            this.log?.('MERGE_SKIPPED', { reason: 'remote_not_newer', localVersion: this.version, remoteVersion: otherDb.version, localMod: this.lastModified, remoteMod: otherDb.lastModified });
            return changes;
        }

        const otherEntries = otherDb.entries || await otherDb._getAllEntries(); // Assume otherDb might be from JSON
        
        // Use batch for merging entries
        const batch = this.db.batch();
        
        for (const [key, remoteEntry] of Object.entries(otherEntries)) {
            // Skip internal metadata keys if they somehow appear in entries
            if (key === this.METADATA_VERSION_KEY || key === this.METADATA_LAST_MODIFIED_KEY) continue;

            try {
                const localEntry = await this.db.get(key).catch(() => null);

                // Merge logic: remote is newer if no local entry OR remote timestamp is later
                if (!localEntry || new Date(localEntry.timestamp) < new Date(remoteEntry.timestamp)) {
                    batch.put(key, remoteEntry);
                    changes++;
                }
            } catch (err) {
                 // Log error getting local key during merge but continue if possible
                 console.error(`Error getting local key ${key} during merge:`, err);
            }
        }

        if (changes > 0) {
            requiresMetadataUpdate = true; // Mark that we need to update metadata
        }

        // Determine the new metadata values *before* writing the batch
        const newVersion = Math.max(this.version, otherDb.version) + (changes > 0 ? 1 : 0); // Increment only if changes were made
        const newLastModified = Date.now(); // Always update lastModified if merging potentially newer data

        // Add metadata to the batch *if* version or changes occurred
        // Always update lastModified if the remote had a newer timestamp, even if no entries changed locally
         if (newVersion > this.version || otherDb.lastModified > this.lastModified) {
             batch.put(this.METADATA_VERSION_KEY, newVersion);
             batch.put(this.METADATA_LAST_MODIFIED_KEY, newLastModified);
             requiresMetadataUpdate = true; // Ensure flag is set if only version/timestamp changes
             console.log(`[merge] Metadata update triggered. Writing version: ${newVersion}`);
         } else {
             console.log(`[merge] Metadata update not triggered. Local version: ${this.version}, Remote version: ${otherDb.version}, Changes: ${changes}, Remote lastModified <= Local lastModified.`);
         }


        // Write the batch if there were entry changes OR metadata updates needed
        if (changes > 0 || requiresMetadataUpdate) {
            console.log(`[merge] Attempting to write batch. Changes: ${changes}, MetadataUpdate: ${requiresMetadataUpdate}`);
            await batch.write();
            console.log(`[merge] Successfully wrote batch. New version in memory will be: ${newVersion}`);

            // Update in-memory state *after* successful write
            this.version = newVersion;
            this.lastModified = newLastModified;
        } else {
             this.log?.('MERGE_INFO', { reason: 'no_newer_entries_or_metadata', localVersion: this.version, remoteVersion: otherDb.version });
        }

        return changes; // Return number of entries changed/added
    } catch (err) {
        console.error('[merge] Error during merge process:', err);
        // Attempt to log using the client's logger if available (passed via context or DI)
        this.log?.('MERGE_ERROR', { error: err.message, stack: err.stack });
        return 0; // Return 0 changes on error
    }
}

  // Export as JSON (includes metadata from memory)
  async toJSON() {
    if (!this.db) throw new Error('Database not initialized');
    const entries = await this._getAllEntries();
    // Filter out internal metadata keys from the entries export
    delete entries[this.METADATA_VERSION_KEY];
    delete entries[this.METADATA_LAST_MODIFIED_KEY];
    return {
      entries,
      version: this.version, // Use in-memory version
      lastModified: this.lastModified, // Use in-memory lastModified
      nodeId: this.nodeId
    };
  }

  // Create from JSON - Less critical now, but ensure it writes metadata if used
  static async fromJSON(json, sourceNodeId) {
    const db = new P2PDatabase(sourceNodeId || json.nodeId || nodeId);
    try {
      await db.open(); // Open first (which might load existing metadata)
      
      const entries = json.entries || {};
      const batch = db.db.batch();
      let entryCount = 0;
      for (const [key, value] of Object.entries(entries)) {
         // Skip internal metadata keys if present in the JSON entries
         if (key === db.METADATA_VERSION_KEY || key === db.METADATA_LAST_MODIFIED_KEY) continue;
         if (key !== '__test__') {
             batch.put(key, value);
             entryCount++;
         }
      }
      
      // Use version/lastModified from JSON, overriding what open() might have loaded
      db.version = json.version || 0; 
      db.lastModified = json.lastModified || Date.now();
      
      // Write metadata from JSON to the DB as well
      batch.put(db.METADATA_VERSION_KEY, db.version);
      batch.put(db.METADATA_LAST_MODIFIED_KEY, db.lastModified);
      
      await batch.write(); // Write entries and metadata
      
      console.log(`Created DB from JSON. Wrote ${entryCount} entries. Version: ${db.version}`);
      return db;
    } catch (err) {
       // ... existing error handling ...
    }
  }

  // Close the database
  async close() {
    if (this.db) {
      try {
        await this.db.close();
        this.db = null;
      } catch (err) {
        console.error('Error closing database:', err);
        throw err;
      }
    }
  }
}

// P2P Database API class
class P2PDBClient {
  constructor(options = {}) {
    // If a nodeId is provided in options, save it to the persistent storage
    if (options.nodeId) {
      const nodeIdFile = path.join(storageDir, 'node-id.json');
      fs.writeFileSync(nodeIdFile, JSON.stringify({ nodeId: options.nodeId }));
      nodeId = options.nodeId; // Update global nodeId
    }
    
    this.nodeId = options.nodeId || nodeId;
    this.storageDir = options.storageDir || storageDir;
    this.topicString = options.topic || 'p2p-database-example';
    this.debug = options.debug || false;
    
    // Initialize logger
    this.logger = new Logger({
      debug: this.debug,
      logFile: path.join(this.storageDir, `sync_debug_${this.nodeId}.jsonl`),
      console: this.debug
    });
    
    nodeId = this.nodeId;
    
    console.log('Node ID:', this.nodeId);
    
    // Initialize database
    this.db = null;
    this.initializeDatabase();
    
    // Track connections
    this.connections = new Set();
    
    // Setup networking with Hyperswarm
    this.swarm = null;
    this.topic = null;
    
    // Add stats tracking
    this.stats = {
      syncsSent: 0,
      syncsReceived: 0,
      changesMerged: 0,
      connectionsTotal: 0,
      errors: 0,
      lastSyncAt: null,
      lastChangeAt: null,
      memoryUsage: {}
    };

    // Track sync chains
    this.activeSyncs = new Map(); // Track active syncs by peer
    
    // Optimization 1: Sync cooldown - prevent excessive syncs with the same peer
    this.lastSyncTimes = new Map(); // Track last sync time per peer
    this.syncCooldown = 5000; // 5 seconds minimum between syncs with the same peer
    
    // Optimization 2: Version based sync - only sync if DB version changed
    this.peerVersions = new Map(); // Track last known DB version per peer
    
    // Setup memory usage tracking and cleanup
    this.memoryCheckInterval = setInterval(() => this.checkMemoryUsage(), 60000);
  }
  
  async initializeDatabase() {
    try {
      // Directly create and open the LevelDB database.
      // This avoids loading the entire dataset from a JSON file into memory at startup.
      this.db = new P2PDatabase(this.nodeId);
      await this.db.open(); // Open the LevelDB store directly
      console.log('Database initialized directly from LevelDB storage. Version:', this.db.version);
      // No need to call saveDatabase() here on startup.
    } catch (err) {
      console.error('Error initializing database:', err);
      throw err;
    }
  }
  
  // Log debug messages if debug mode is on
  log(type, data) {
    this.logger.log(type, {
      nodeId: this.nodeId,
      ...data
    });
  }
  
  // Get stats about the P2P connection
  async getStats() {
    const entries = await this.db._getAllEntries();
    const activeEntries = Object.entries(entries).filter(([_, entry]) => !entry.deleted).length;
    const memoryUsage = process.memoryUsage();
    return {
      ...this.stats,
      connectionsActive: this.connections.size,
      databaseVersion: this.db.version,
      databaseEntries: activeEntries,
      databaseLastModified: new Date(this.db.lastModified).toISOString(),
      nodeId: this.nodeId,
      memory: {
        heapTotal: Math.round(memoryUsage.heapTotal / 1024 / 1024) + ' MB',
        heapUsed: Math.round(memoryUsage.heapUsed / 1024 / 1024) + ' MB',
        rss: Math.round(memoryUsage.rss / 1024 / 1024) + ' MB',
        external: Math.round(memoryUsage.external / 1024 / 1024) + ' MB'
      }
    };
  }
  
  // Function to generate a save file path
  getSaveFilePath() {
    return path.join(this.storageDir, `db-${this.nodeId}.json`);
  }
  
  // Function to save database snapshot to disk
  // NOTE: This function still loads the entire database into memory via toJSON()
  // Call it judiciously (e.g., on shutdown or for manual backups).
  async saveDatabase() {
    try {
      const dbJson = await this.db.toJSON(); // This loads all entries into memory!
      const data = JSON.stringify(dbJson, null, 2);
      fs.writeFileSync(this.getSaveFilePath(), data);
      console.log('Saved database snapshot to disk:', this.getSaveFilePath());
    } catch (err) {
        console.error('Error saving database snapshot:', err);
    }
  }
  
  // Function to list all entries
  async listAllEntries() {
    const entries = await this.db._getAllEntries();
    console.log('Last modified:', new Date(this.db.lastModified).toISOString());
    return entries;
  }
  
  // Function to handle sync with a new peer
  async handleSyncRequest(socket, trigger = 'unknown') {
    // Get the most reliable peer ID we have for this socket *at this moment*.
    // It might be 'unknown' initially. The check relies on this possibly stale ID.
    const peerIdForLookup = socket._peerNodeId || 'unknown'; 
    
    try {
      // Optimization: Prevent sync loops - check if we've recently synced with this peer
      const now = Date.now();
      const lastSyncTime = this.lastSyncTimes.get(peerIdForLookup) || 0;
      
      // Skip if we've synced with this peer recently unless this is an initial connection
      if (trigger !== 'initial_connection' && 
          now - lastSyncTime < this.syncCooldown) {
        this.log('SYNC_SKIPPED', {
          peerId: peerIdForLookup,
          trigger,
          reason: 'cooldown',
          timeSinceLastSync: now - lastSyncTime
        });
        return;
      }
      
      // Track sync chain to detect loops
      const syncChain = socket._syncChain || [];
      
      // Check if our ID is already in the chain (prevent sync loops)
      if (syncChain.includes(this.nodeId)) {
        this.log('SYNC_SKIPPED', {
          peerId: peerIdForLookup,
          trigger,
          reason: 'loop_detected',
          syncChain: syncChain
        });
        return;
      }
      
      // Update sync chain with our ID
      socket._syncChain = [...syncChain, this.nodeId];
      
      this.log('SYNC_START', {
        peerId: peerIdForLookup,
        trigger,
        syncChain: socket._syncChain,
        activeConnections: this.connections.size
      });
      
      // *** Use consistent peerIdForLookup for version check ***
      const currentVersion = this.db.version;
      const currentLastModified = this.db.lastModified;
      // Get the last known version using the ID associated with the socket
      const peerLastVersion = this.peerVersions.get(peerIdForLookup) || 0; 

      let messagePayload;
      let messageType = 'metadata_only'; 

      // Decide based on the potentially stale peerLastVersion from the map
      if (trigger === 'initial_connection' || currentVersion > peerLastVersion) {
        this.log('SYNC_PREPARING_FULL', { peerId: peerIdForLookup, trigger, localVersion: currentVersion, peerKnownVersion: peerLastVersion });
        messageType = 'full_state';
        const dbState = await this.db.toJSON(); 
        messagePayload = {
          ...dbState, 
          senderNodeId: this.nodeId, // Send our actual ID
          syncChain: socket._syncChain 
        };
      } else {
        this.log('SYNC_PREPARING_METADATA', { peerId: peerIdForLookup, trigger, localVersion: currentVersion, peerKnownVersion: peerLastVersion });
        messagePayload = {
          senderNodeId: this.nodeId, // Send our actual ID
          syncChain: socket._syncChain,
          version: currentVersion,
          lastModified: currentLastModified,
          entries: null 
        };
      }
      // *** END CHANGE ***

      this.log('SYNC_START', { 
        peerId: peerIdForLookup, // Log the ID used for the check
        trigger,
        messageType, 
        syncChain: socket._syncChain,
        activeConnections: this.connections.size
      });
            
      const message = JSON.stringify(messagePayload);
      
      socket.write(message);
      this.stats.syncsSent++;
      this.stats.lastSyncAt = new Date().toISOString();
      
      // Update lastSyncTimes using the same potentially 'unknown' ID for consistency with the check
      this.lastSyncTimes.set(peerIdForLookup, Date.now()); 
      
      this.log('SYNC_SENT', {
        peerId: peerIdForLookup, // Log the ID used for check/sync time
        trigger,
        messageType,
        dataSize: message.length,
        syncChain: socket._syncChain,
        version: currentVersion 
      });
      
      // For initial connections only, request state back after a delay
      if (trigger === 'initial_connection') {
        setTimeout(() => {
          if (!socket.destroyed) {
            const requestPayload = { 
              requestSync: true,
              senderNodeId: this.nodeId, 
              trigger: 'response_request',
              syncChain: socket._syncChain 
            };
            socket.write(JSON.stringify(requestPayload));
            
            this.log('SYNC_REQUEST', {
              // Log the peer we are requesting from, ID might still be unknown here
              peerId: socket._peerNodeId || 'unknown', 
              trigger: 'response_request',
              syncChain: socket._syncChain
            });
          }
        }, 500);
      }
    } catch (err) {
       this.log('SYNC_ERROR', {
        peerId: socket._peerNodeId || 'unknown', 
        error: err.message,
        stack: err.stack,
        trigger
      });
      this.stats.errors++;
    }
  }
  
  // Handle incoming data from peers
  async handlePeerData(socket, _, data) {
      // Default peerId from socket, might be 'unknown'
      let currentPeerId = socket._peerNodeId || 'unknown'; 
      
      try {
          const parsed = JSON.parse(data);
          
          // *** Prioritize senderNodeId from the message for all subsequent operations ***
          const messageSenderId = parsed.senderNodeId;
          let peerId = currentPeerId; // Start with socket's known ID

          if (messageSenderId) {
              // If the message has an ID, ALWAYS use it.
              peerId = messageSenderId; 
              if (messageSenderId !== currentPeerId) {
                  // If it differs from the socket's known ID, update the socket's record
                  socket._peerNodeId = messageSenderId;
                  this.log('PEER_ID_UPDATED', { connectionId: 'SocketAssociation', oldId: currentPeerId, newId: messageSenderId });
                  
                  // Optional: Clean up old 'unknown' or incorrect entries if messageSenderId was previously associated differently
                  if (currentPeerId !== 'unknown') {
                     this.lastSyncTimes.delete(currentPeerId);
                     this.peerVersions.delete(currentPeerId);
                  }
              }
          } else if (peerId === 'unknown') {
              // Still unknown and message has no ID - this is problematic
              this.log('PEER_DATA_WARNING', { message: "Received data without senderNodeId from unidentified peer. Ignoring.", rawDataSample: data.toString('utf8').substring(0, 200) });
              return; // Ignore data if we can't identify the sender
          }
          // From here, 'peerId' reliably holds the sender's ID if provided, or the socket's last known ID otherwise

          socket._syncChain = parsed.syncChain || []; 
          
          this.stats.syncsReceived++;
          
          const messageType = parsed.entries ? 'full_state' : (parsed.requestSync ? 'sync_request' : 'metadata_only');

          this.log('PEER_DATA', {
              peerId, // Use the determined peerId
              messageType,
              syncChain: socket._syncChain,
              dataSize: data.length 
          });

          if (parsed.requestSync) {
              const trigger = parsed.trigger || 'request';
              // Let handleSyncRequest use the (now hopefully updated) socket._peerNodeId
              await this.handleSyncRequest(socket, `peer_request_${trigger}`); 
              return;
          }

          // Update peerVersions map using the authoritative peerId
          if (parsed.version !== undefined) {
              this.peerVersions.set(peerId, parsed.version); 
              this.log('PEER_VERSION_UPDATE', { peerId: peerId, version: parsed.version }); // Log version updates
          }
          
          // --- Check if we need to request a full sync based on metadata ---
          const remoteVersion = parsed.version;
          const remoteLastModified = parsed.lastModified;
          const localVersion = this.db.version;
          const localLastModified = this.db.lastModified;

          if (!parsed.entries && // Only act on metadata-only messages or potentially empty full messages
              (remoteVersion > localVersion || remoteLastModified > localLastModified)) {
              
              // Check cooldown before requesting again from this specific peer
              const now = Date.now();
              const lastSyncReqTime = socket._lastSyncRequestTime || 0; // Track last request time on the socket
              const syncRequestCooldown = 10000; // e.g., 10 seconds cooldown for requesting

              if (now - lastSyncReqTime > syncRequestCooldown) {
                  this.log('REQUESTING_SYNC_FROM_PEER', {
                      peerId,
                      reason: 'remote_newer_metadata',
                      remoteVersion, remoteLastModified,
                      localVersion, localLastModified
                  });

                  const requestPayload = {
                      requestSync: true,
                      senderNodeId: this.nodeId,
                      trigger: 'metadata_update_request',
                      syncChain: parsed.syncChain || [] // Pass along the chain
                  };
                  socket.write(JSON.stringify(requestPayload));
                  socket._lastSyncRequestTime = now; // Update last request time
                  
                  // Since we requested a sync, we don't need to proceed with merging *this* metadata message
                  // The peer will send back a full state shortly.
                  return; 
              } else {
                  this.log('REQUEST_SYNC_SKIPPED', {
                      peerId,
                      reason: 'request_cooldown',
                      timeSinceLastReq: now - lastSyncReqTime
                  });
              }
          }
          // --- End Check ---

          // --- Merge Logic --- 
          // If we received entries (full state), proceed to merge
          if (parsed.entries) {
              this.db.log = this.log.bind(this); 

              let changes = 0;
              const remoteDbState = {
                  entries: parsed.entries || {}, 
                  version: parsed.version,
                  lastModified: parsed.lastModified, 
                  nodeId: peerId // Use the authoritative peerId
              };

              changes = await this.db.merge(remoteDbState);

              delete this.db.log; 
              // --- End Merge Logic ---

              this.log('MERGE_RESULT', { 
                  peerId, // Use authoritative peerId
                  changes,
                  newLocalVersion: this.db.version, 
                  syncChain: socket._syncChain
              });

              if (changes > 0) {
                  console.log(`Merged ${changes} changes from ${peerId}. New version: ${this.db.version}`);
                  this.stats.changesMerged += changes;
                  this.stats.lastChangeAt = new Date().toISOString();

                  // Notify peers logic
                  const otherPeers = Array.from(this.connections)
                      .filter(s => s !== socket && !s.destroyed);
                  let peersToNotify = otherPeers;
                  if (otherPeers.length > 3) {
                      peersToNotify = otherPeers.sort(() => Math.random() - 0.5).slice(0, 3);
                  }
                  
                  this.log('NOTIFY_PEERS', {
                      sourcePeerId: peerId, // Use authoritative peerId
                      peerCount: peersToNotify.length,
                      totalPeers: otherPeers.length,
                      changes,
                      syncChain: socket._syncChain 
                  });
                  
                  for (const otherSocket of peersToNotify) {
                      // The handleSyncRequest call here will use otherSocket._peerNodeId for its lookup,
                      // which should have been updated if data was received from that peer.
                       await this.handleSyncRequest(otherSocket, `changes_from_${peerId}`); // Pass authoritative source peerId
                  }
              }
          }

      } catch (err) {
         this.log('PEER_DATA_ERROR', {
              peerId: socket._peerNodeId || 'unknown', // Use socket's ID on error
              error: err.message,
              stack: err.stack,
              rawDataSample: data.toString('utf8').substring(0, 200) 
          });
          this.stats.errors++;
          if (this.db) delete this.db.log;
      }
  }

  // Start the P2P network
  async start() {
    try {
      // Setup networking with Hyperswarm
      this.swarm = new Hyperswarm();
      
      // Use a fixed topic for discovery
      this.topic = crypto.createHash('sha256').update(this.topicString).digest();
      console.log('Using topic:', this.topic.toString('hex'));
      
      // Handle new connections
      this.swarm.on('connection', (socket, info) => {
        try {
          // We'll get the actual nodeId from the sync messages
          this.connections.add(socket);
          this.stats.connectionsTotal++;
          
          this.log('NEW_CONNECTION', {
            peerId: socket._peerNodeId || 'unknown',
            peerInfo: info
          });
          
          // Initial sync - share our database state
          this.handleSyncRequest(socket, 'initial_connection').catch(err => {
            console.error('Error in initial sync:', err);
            this.stats.errors++;
          });
          
          // Handle incoming data from peers
          let buffer = '';
          let peerNodeId = null;
          
          socket.on('data', data => {
            (async () => {
              try {
                buffer += data.toString('utf8');
                
                try {
                  // Try to parse the buffer as JSON
                  const parsed = JSON.parse(buffer);
                  // If successful, reset the buffer
                  buffer = '';
                  
                  // Update peer's nodeId if we receive it
                  if (parsed.senderNodeId && !peerNodeId) {
                    peerNodeId = parsed.senderNodeId;
                    socket._peerNodeId = peerNodeId;
                    this.log('NEW_PEER_IDENTIFIED', {
                      peerId: peerNodeId
                    });
                  }
                  
                  await this.handlePeerData(socket, null, JSON.stringify(parsed));
                } catch (parseErr) {
                  // If we can't parse the JSON yet, it might be an incomplete message
                  // We'll keep the buffer and wait for more data
                  this.log('COULD_NOT_PARSE_BUFFER', {
                    buffer: buffer.length
                  });
                  
                  // But if the buffer gets too large, clear it to prevent memory issues
                  if (buffer.length > 1000000) { // 1MB limit
                    console.error('Buffer too large, clearing');
                    buffer = '';
                    this.stats.errors++;
                  }
                }
              } catch (err) {
                console.error('Error processing peer data:', err);
                buffer = ''; // Reset buffer on error
                this.stats.errors++;
              }
            })().catch(err => {
              console.error('Error in data handler:', err);
              this.stats.errors++;
            });
          });
          
          // Handle disconnection
          socket.on('close', () => {
            if (peerNodeId) {
              console.log('Peer disconnected:', peerNodeId);
            }
            this.connections.delete(socket);
          });
          
          socket.on('error', (err) => {
            console.error('Socket error:', err);
            this.connections.delete(socket);
            this.stats.errors++;
          });
        } catch (err) {
          console.error('Error handling connection:', err);
          this.stats.errors++;
        }
      });
      
      // Handle discovery events
      this.swarm.on('peer', (peer) => {
        try {
          const peerId = typeof peer.publicKey === 'object' ? 
            peer.publicKey.toString('hex').slice(0, 8) : 
            peer.toString('hex').slice(0, 8);
          console.log('Discovered peer:', peerId);
        } catch (err) {
          console.log('Discovered peer (format unknown)');
        }
      });
      
      // Handle errors
      this.swarm.on('error', (err) => {
        console.error('Swarm error:', err.message);
      });
      
      // Start listening
      this.swarm.listen();
      console.log('Swarm listening for connections');
      
      // Join the topic to discover other peers
      this.swarm.join(this.topic, { server: true, client: true });
      console.log('Joined swarm with topic');
      
      // Initial peer discovery
      await this.swarm.flush();
      console.log('Initial peer discovery completed');
      
      // Add periodic sync every 30 seconds for reliability
      this.syncInterval = setInterval(() => {
        if (this.connections.size > 0) {
          //console.log('Performing periodic sync...');
          this.syncWithPeers('periodic'); // Pass 'periodic' trigger
        }
      }, 30000);
      
      console.log('\nDatabase ready for use.');
      console.log('You can now add, update, or delete entries via the API.');
      console.log('Changes will automatically be synced with peers.');
      await this.listAllEntries();
      
      return true;
    } catch (err) {
      console.error('Error starting P2P network:', err.message);
      return false;
    }
  }
  
  // Stop the P2P network
  async stop() {
    try {
      console.log('\nClosing...');
      
      if (this.syncInterval) {
        clearInterval(this.syncInterval);
      }
      
      if (this.memoryCheckInterval) {
        clearInterval(this.memoryCheckInterval);
      }
      
      for (const socket of this.connections) {
        socket.destroy();
      }
      
      // Clear references to help garbage collection
      this.connections.clear();
      this.lastSyncTimes.clear();
      this.peerVersions.clear();
      
      // Save final state snapshot and close database
      console.log('Saving final database snapshot before closing...');
      await this.saveDatabase(); // Save JSON snapshot on graceful shutdown
      await this.db.close();     // Close the LevelDB instance
      
      if (this.swarm) {
        await this.swarm.destroy();
      }
      
      console.log('P2P network stopped');
      return true;
    } catch (err) {
      console.error('Error stopping P2P network:', err);
      return false;
    }
  }
  
  // Memory optimization check
  checkMemoryUsage() {
    const memoryUsage = process.memoryUsage();
    this.stats.memoryUsage = {
      heapTotal: Math.round(memoryUsage.heapTotal / 1024 / 1024) + ' MB',
      heapUsed: Math.round(memoryUsage.heapUsed / 1024 / 1024) + ' MB',
      rss: Math.round(memoryUsage.rss / 1024 / 1024) + ' MB',
      external: Math.round(memoryUsage.external / 1024 / 1024) + ' MB'
    };
    
    this.log('MEMORY_USAGE', {
      ...this.stats.memoryUsage,
      connections: this.connections.size,
      syncTimes: this.lastSyncTimes.size,
      peerVersions: this.peerVersions.size
    });
    
    // If memory usage is high, perform cleanup
    if (memoryUsage.heapUsed > 200 * 1024 * 1024) { // 200MB
      this.performMemoryCleanup();
    }
    
    // Force garbage collection if available (node --expose-gc)
    if (global.gc) {
      global.gc();
    }
  }
  
  // Memory cleanup procedures
  performMemoryCleanup() {
    this.log('MEMORY_CLEANUP', {
      before: this.stats.memoryUsage
    });
    
    // Clean up old entries in the tracking maps
    const now = Date.now();
    const oldThreshold = now - (30 * 60 * 1000); // 30 minutes
    
    // Clean up inactive peer sync records
    let cleanedPeers = 0;
    for (const [peerId, lastTime] of this.lastSyncTimes.entries()) {
      if (lastTime < oldThreshold) {
        this.lastSyncTimes.delete(peerId);
        this.peerVersions.delete(peerId);
        cleanedPeers++;
      }
    }
    
    // Re-check memory after cleanup
    const memoryAfter = process.memoryUsage();
    this.log('MEMORY_CLEANUP_COMPLETE', {
      cleanedPeers,
      before: this.stats.memoryUsage,
      after: {
        heapTotal: Math.round(memoryAfter.heapTotal / 1024 / 1024) + ' MB',
        heapUsed: Math.round(memoryAfter.heapUsed / 1024 / 1024) + ' MB',
        rss: Math.round(memoryAfter.rss / 1024 / 1024) + ' MB',
        external: Math.round(memoryAfter.external / 1024 / 1024) + ' MB'
      }
    });
  }
  
  // Database operations
  // Function to add an entry
  async addEntry(infohashService, cacheStatus, expiration) {
    try {
      // Only handle expiration logic, keep cacheStatus as is
      let normalizedExpiration = expiration;
      
      if (typeof cacheStatus === 'boolean' && !expiration) {
        normalizedExpiration = cacheStatus ? 
          new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString() : // 7 days for cached items
          new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();      // 24 hours for non-cached items
      }
      
      const result = await this.db.addEntry(infohashService, cacheStatus, normalizedExpiration);
      
      if (result) {
        console.log(`Added entry: ${infohashService}`);
        // await this.saveDatabase(); // REMOVED: LevelDB persists automatically
        // await this.listAllEntries(); // NOTE: listAllEntries still loads all entries for display. Consider removing if not essential.
        
        // Notify peers of the change
        await this.syncWithPeers(`add_${infohashService}`);
      }
      
      return result;
    } catch (err) {
      console.error('Failed to add entry:', err.message);
      return false;
    }
  }
  
  // Function to update an entry
  async updateEntry(infohashService, cacheStatus, expiration) {
    try {
      // Only handle expiration logic, keep cacheStatus as is
      let normalizedExpiration = expiration;
      
      if (typeof cacheStatus === 'boolean' && !expiration) {
        normalizedExpiration = cacheStatus ? 
          new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString() : // 7 days for cached items
          new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();      // 24 hours for non-cached items
      }
      
      const result = await this.db.updateEntry(infohashService, cacheStatus, normalizedExpiration);
      
      if (result) {
        console.log(`Updated entry: ${infohashService}`);
        // await this.saveDatabase(); // REMOVED: LevelDB persists automatically
        // await this.listAllEntries(); // NOTE: listAllEntries still loads all entries for display. Consider removing if not essential.
        
        // Notify peers of the change
        await this.syncWithPeers(`update_${infohashService}`);
      } else {
        console.error(`Entry not found: ${infohashService}`);
      }
      
      return result;
    } catch (err) {
      console.error('Failed to update entry:', err.message);
      return false;
    }
  }
  
  // Function to delete an entry
  async deleteEntry(infohashService) {
    try {
      const result = await this.db.deleteEntry(infohashService);
      
      if (result) {
        console.log(`Deleted entry: ${infohashService}`);
        // await this.saveDatabase(); // REMOVED: LevelDB persists automatically
        // await this.listAllEntries(); // NOTE: listAllEntries still loads all entries for display. Consider removing if not essential.
        
        // Notify peers of the change
        await this.syncWithPeers(`delete_${infohashService}`);
      } else {
        console.error(`Entry not found: ${infohashService}`);
      }
      
      return result;
    } catch (err) {
      console.error('Failed to delete entry:', err.message);
      return false;
    }
  }
  
  // Get all entries
  async getAllEntries() {
    const entries = await this.db._getAllEntries();
    const activeEntries = {};
    
    for (const [key, entry] of Object.entries(entries)) {
      if (!entry.deleted) {
        activeEntries[key] = entry;
      }
    }
    
    return activeEntries;
  }
  
  // Get a specific entry
  async getEntry(infohashService) {
    try {
      const entry = await this.db.db.get(infohashService);
      if (entry && !entry.deleted) {
        return entry;
      }
    } catch (err) {
      if (!err.notFound) {
        console.error('Error getting entry:', err);
      }
    }
    return null;
  }

  // Function to get all entries in the structured format
  async getAllEntriesStructured() {
    const entries = await this.getAllEntries();
    return this.transformToStructuredFormat(entries);
  }
  
  // Function to transform entries from internal format to structured format
  transformToStructuredFormat(entries) {
    const data = [];
    const infohashMap = new Map();
    
    // Group by infohash, excluding deleted entries
    for (const [key, entry] of Object.entries(entries)) {
      // Skip deleted entries
      if (entry.deleted) continue;
      
      // Parse the infohash+service key
      const [infohash, service] = key.split('+');
      
      // Get or create the infohash entry
      if (!infohashMap.has(infohash)) {
        const infohashEntry = { 
          infohash, 
          services: {} 
        };
        data.push(infohashEntry);
        infohashMap.set(infohash, infohashEntry);
      }
      
      // Add the service data
      const infohashEntry = infohashMap.get(infohash);
      
      // Convert any string status to boolean if needed
      let cached = entry.cacheStatus;
      if (typeof cached === 'string') {
        cached = cached === 'completed' || cached === 'cached';
      }
      
      infohashEntry.services[service] = {
        cached: cached,
        last_modified: entry.timestamp,
        expiry: entry.expiration
      };
    }
    
    return { data };
  }
  
  // Function to transform from structured format to internal format
  transformFromStructuredFormat(structuredData) {
    const entries = {};
    
    if (structuredData.data && Array.isArray(structuredData.data)) {
      for (const item of structuredData.data) {
        const infohash = item.infohash;
        
        if (item.services) {
          for (const [service, serviceData] of Object.entries(item.services)) {
            const key = `${infohash}+${service}`;
            
            // Keep cache status as is (boolean)
            const isCached = typeof serviceData.cached === 'boolean' ? 
              serviceData.cached : 
              (serviceData.cached === 'true' || serviceData.cached === 'completed' || serviceData.cached === 'cached');
            
            // Set expiration based on cache status
            const defaultExpiration = isCached ? 
              new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString() : // 7 days for cached items
              new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();      // 24 hours for non-cached items
            
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
  
  // Add entries from structured format
  async addEntriesStructured(structuredData) {
    const entries = this.transformFromStructuredFormat(structuredData);
    const results = [];
    
    for (const [key, entry] of Object.entries(entries)) {
      const result = await this.addEntry(key, entry.cacheStatus, entry.expiration);
      results.push({ key, success: result });
    }
    
    return results;
  }
  
  // Update entries from structured format
  async updateEntriesStructured(structuredData) {
    const entries = this.transformFromStructuredFormat(structuredData);
    const results = [];
    
    for (const [key, entry] of Object.entries(entries)) {
      const result = await this.updateEntry(key, entry.cacheStatus, entry.expiration);
      results.push({ key, success: result });
    }
    
    return results;
  }

  // Function to sync database state with all connected peers
  async syncWithPeers(trigger = 'local_change') {
    this.log('SYNC_TRIGGERED', { trigger, peerCount: this.connections.size });
    for (const socket of this.connections) {
      if (!socket.destroyed) {
        // Use the peerId associated with the socket for logging/checks if available
        const peerId = socket._peerNodeId || 'unknown';
        try {
          // Pass the trigger reason to handleSyncRequest
          await this.handleSyncRequest(socket, trigger);
        } catch (err) {
          this.log('SYNC_PEER_ERROR', { peerId, trigger, error: err.message });
          this.stats.errors++;
          // Decide if we should remove the connection on error, e.g.:
          // this.connections.delete(socket);
          // socket.destroy();
        }
      } else {
        // Clean up destroyed sockets if they are still in the set
        this.connections.delete(socket);
      }
    }
  }
}

// Export the API
module.exports = {
  P2PDBClient
};

// Run as standalone if executed directly
if (require.main === module) {
  // Create a client instance
  const client = new P2PDBClient();
  
  // Start the P2P network
  client.start()
    .then(async () => {
      // Add a test entry
      await client.addEntry('infohash1+web', 'pending', null);
    })
    .catch(console.error);
  
  // Handle cleanup on exit
  process.on('SIGINT', async () => {
    await client.stop();
    process.exit();
  });
} 