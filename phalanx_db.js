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
          // Check if the loaded value is a valid number
          if (typeof storedVersion === 'number' && !isNaN(storedVersion)) {
            this.version = storedVersion;
            console.log(`Loaded existing DB version: ${this.version}`);
          } else {
            // Loaded value is invalid or not a number
            const originalValue = storedVersion; // Capture for logging
            this.version = 0; // Default to 0
            console.warn(`Invalid or non-numeric stored version value encountered ('${originalValue}'). Defaulting to version 0.`);
            // Overwrite the invalid value in the DB with the valid default
            await this.db.put(this.METADATA_VERSION_KEY, this.version);
            console.log('Corrected invalid version value in database.');
          }
        } catch (err) {
          if (err.notFound) {
            console.log('No existing DB version found, starting at 0 and writing to DB.');
            // The initial value (0) set in the constructor is already assigned.
            // Write initial metadata if not found to ensure consistency
            await this.db.put(this.METADATA_VERSION_KEY, this.version);
            // We'll write lastModified below, no need to do a full batch here now
          } else {
            console.error('Error loading DB version:', err);
            // Fallback to 0 on other load errors
            this.version = 0;
            console.warn(`Using version 0 due to error loading version.`);
          }
        }
        try {
           const storedLastModified = await this.db.get(this.METADATA_LAST_MODIFIED_KEY);
           // Check if the loaded value is a valid time value
           if (storedLastModified !== undefined && storedLastModified !== null && !isNaN(new Date(storedLastModified).getTime())) {
             this.lastModified = storedLastModified;
             // Only log with toISOString if it's valid
             console.log(`Loaded existing DB lastModified: ${new Date(this.lastModified).toISOString()}`);
           } else {
             // Loaded value is invalid or couldn't be parsed as a Date
             const originalValue = storedLastModified; // Capture for logging
             this.lastModified = Date.now(); // Default to current time
             console.warn(`Invalid or unparseable stored lastModified value encountered ('${originalValue}'). Defaulting to current time: ${new Date(this.lastModified).toISOString()}`);
             // Overwrite the invalid value in the DB with the valid default
             await this.db.put(this.METADATA_LAST_MODIFIED_KEY, this.lastModified);
             console.log('Corrected invalid lastModified value in database.');
           }
        } catch (err) {
             if (err.notFound) {
               // Key not found, the initial value (Date.now()) set in the constructor is already assigned.
               console.log('No existing DB lastModified found, using current time and writing to DB.');
               // Add the key if it's missing to prevent future notFound errors and ensure consistency
               await this.db.put(this.METADATA_LAST_MODIFIED_KEY, this.lastModified);
             } else {
               // Other error during loading
               console.error('Error loading DB lastModified:', err);
               // Fallback to current time even on other load errors? Yes, probably safest.
               this.lastModified = Date.now();
               console.warn(`Using current time due to error loading lastModified.`);
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
  
  // Function to handle sync with a new peer (Sends Metadata Only Initially)
  async handleSyncRequest(socket, trigger = 'unknown') {
    const peerIdForLookup = socket._peerNodeId || 'unknown';

    try {
      // Cooldown check
      const now = Date.now();
      const lastSyncTime = this.lastSyncTimes.get(peerIdForLookup) || 0;
      if (trigger !== 'initial_connection' && now - lastSyncTime < this.syncCooldown) {
        this.log('SYNC_SKIPPED', { peerId: peerIdForLookup, trigger, reason: 'cooldown', timeSinceLastSync: now - lastSyncTime });
        return;
      }

      // Sync chain loop detection
      const syncChain = socket._syncChain || [];
      if (syncChain.includes(this.nodeId)) {
        this.log('SYNC_SKIPPED', { peerId: peerIdForLookup, trigger, reason: 'loop_detected', syncChain: syncChain });
        return;
      }
      socket._syncChain = [...syncChain, this.nodeId];

      // --- Always send metadata first ---
      const currentVersion = this.db.version;
      const currentLastModified = this.db.lastModified;
      const messageType = 'metadata_only';

      this.log('SYNC_PREPARING_METADATA', { peerId: peerIdForLookup, trigger, localVersion: currentVersion });

      const messagePayload = {
        type: messageType, // Add explicit type
        senderNodeId: this.nodeId,
        syncChain: socket._syncChain,
        version: currentVersion,
        lastModified: currentLastModified,
      };

      this.log('SYNC_START', {
        peerId: peerIdForLookup,
        trigger,
        messageType,
        syncChain: socket._syncChain,
        activeConnections: this.connections.size
      });

      const message = JSON.stringify(messagePayload);
      socket.write(message);
      this.stats.syncsSent++;
      this.stats.lastSyncAt = new Date().toISOString();
      this.lastSyncTimes.set(peerIdForLookup, Date.now());

      this.log('SYNC_SENT', {
        peerId: peerIdForLookup,
        trigger,
        messageType,
        dataSize: message.length,
        syncChain: socket._syncChain,
        version: currentVersion
      });

      // --- Remove automatic request back ---
      // The peer will request sync if needed based on the metadata we just sent.

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

  // Handle incoming data from peers (Handles Metadata, Requests, and Chunks)
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

                  // Optional: Clean up old 'unknown' or incorrect entries
                  if (currentPeerId !== 'unknown') {
                     this.lastSyncTimes.delete(currentPeerId);
                     this.peerVersions.delete(currentPeerId);
                  }
              }
          } else if (peerId === 'unknown') {
              // Still unknown and message has no ID - problematic
              this.log('PEER_DATA_WARNING', { message: "Received data without senderNodeId from unidentified peer. Ignoring.", rawDataSample: data.toString('utf8').substring(0, 200) });
              return; // Ignore data if we can't identify the sender
          }
          // From here, 'peerId' reliably holds the sender's ID

          socket._syncChain = parsed.syncChain || [];

          this.stats.syncsReceived++;

          // Infer message type more robustly
          const messageType = parsed.type || (parsed.entries ? 'full_state' : (parsed.requestSync ? 'sync_request' : 'metadata_only'));

          this.log('PEER_DATA', {
              peerId,
              messageType,
              syncChain: socket._syncChain,
              dataSize: data.length
          });

          // --- Handle Sync Requests ---
          if (messageType === 'sync_request') {
              const requestTrigger = parsed.trigger || 'request';
              const requestType = parsed.requestType || 'metadata'; // Default to requesting metadata

              if (requestType === 'full_state') {
                  // *** Handle request for full state using chunking ***
                  await this.sendFullStateChunks(socket, `peer_request_${requestTrigger}`, parsed.syncChain || []);
              } else {
                  // Existing behavior: send metadata only
                  await this.handleSyncRequest(socket, `peer_request_${requestTrigger}`);
              }
              return;
          }

          // --- Handle Metadata & Full State & Chunks ---

          // Update peerVersions map using the authoritative peerId
          if (parsed.version !== undefined) {
              this.peerVersions.set(peerId, parsed.version);
              this.log('PEER_VERSION_UPDATE', { peerId: peerId, version: parsed.version });
          }

          // --- Check if we need to request a full sync based on metadata ---
          const remoteVersion = parsed.version;
          const remoteLastModified = parsed.lastModified;
          const localVersion = this.db.version;
          const localLastModified = this.db.lastModified;

          // Check only if we received metadata OR an empty full_state message
          if ((messageType === 'metadata_only' || (messageType === 'full_state' && !parsed.entries)) &&
              (remoteVersion > localVersion || remoteLastModified > localLastModified)) {

              // Check cooldown before requesting again from this specific peer
              const now = Date.now();
              const lastSyncReqTime = socket._lastSyncRequestTime || 0;
              const syncRequestCooldown = 10000; // 10 seconds cooldown for requesting

              if (now - lastSyncReqTime > syncRequestCooldown) {
                  this.log('REQUESTING_SYNC_FROM_PEER', {
                      peerId,
                      reason: 'remote_newer_metadata',
                      remoteVersion, remoteLastModified,
                      localVersion, localLastModified
                  });

                  const requestPayload = {
                      type: 'sync_request', // Use type field
                      requestSync: true, // Keep for potential backward compatibility
                      requestType: 'full_state', // *** Explicitly request full state ***
                      senderNodeId: this.nodeId,
                      trigger: 'metadata_update_request',
                      syncChain: parsed.syncChain || [] // Pass along the chain
                  };
                  socket.write(JSON.stringify(requestPayload));
                  socket._lastSyncRequestTime = now; // Update last request time

                  // Since we requested a sync, we don't need to proceed with merging *this* metadata message
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

          // --- Handle Incoming Chunks ---
          if (messageType === 'sync_start') {
              socket._chunkBuffer = []; // Initialize buffer for this sync
              // socket._expectedChunks = parsed.totalChunks; // We don't send totalChunks upfront anymore
              socket._syncMeta = { // Store metadata associated with this sync
                  version: parsed.version,
                  lastModified: parsed.lastModified
              };
              this.log('SYNC_CHUNK_START', { peerId });
              return; // Wait for chunks
          }

          if (messageType === 'sync_chunk') {
              if (!socket._chunkBuffer) {
                  this.log('SYNC_CHUNK_ERROR', { peerId, error: 'Received chunk before sync_start' });
                  // Consider closing the connection or requesting again if this happens
                  return; // Ignore orphan chunk
              }
              // TODO: Add sequence number checking for robustness if needed
              socket._chunkBuffer.push(...parsed.chunk); // Append entries from chunk
              this.log('SYNC_CHUNK_RECEIVED', { peerId, sequence: parsed.sequence, chunkSize: parsed.chunk.length, bufferSize: socket._chunkBuffer.length });
              return; // Wait for more chunks or end
          }

          if (messageType === 'sync_end') {
              if (!socket._chunkBuffer) {
                   this.log('SYNC_CHUNK_ERROR', { peerId, error: 'Received sync_end before sync_start or after error' });
                   return;
              }
              if (socket._chunkBuffer.length !== parsed.totalEntries) {
                   this.log('SYNC_CHUNK_ERROR', {
                       peerId,
                       error: 'Received sync_end with mismatched entry count',
                       expected: parsed.totalEntries,
                       received: socket._chunkBuffer.length
                   });
                   socket._chunkBuffer = null; // Clear buffer on error
                   socket._syncMeta = null;
                   return; // Don't merge incomplete data
              }

              this.log('SYNC_CHUNK_END', { peerId, totalEntries: socket._chunkBuffer.length });

              // Assemble the full state from chunks
              const assembledEntries = {};
              for (const entry of socket._chunkBuffer) {
                  // Ensure entry has key and value before assigning
                  if (entry && entry.key !== undefined && entry.value !== undefined) {
                      assembledEntries[entry.key] = entry.value;
                  } else {
                      this.log('SYNC_CHUNK_WARNING', { peerId, message: 'Skipping invalid entry in chunk buffer', entry: entry });
                  }
              }
              const receivedVersion = parsed.version !== undefined ? parsed.version : (socket._syncMeta ? socket._syncMeta.version : 0);
              const receivedLastModified = parsed.lastModified !== undefined ? parsed.lastModified : (socket._syncMeta ? socket._syncMeta.lastModified : 0);

              socket._chunkBuffer = null; // Clear buffer
              socket._syncMeta = null;    // Clear sync metadata

              // Prepare remoteDbState for merge
              const remoteDbState = {
                  entries: assembledEntries,
                  version: receivedVersion,
                  lastModified: receivedLastModified,
                  nodeId: peerId
              };

              // Proceed to merge
              await this.mergeReceivedState(remoteDbState, socket, parsed.syncChain);
              return; // Merge complete
          }
          // --- End Chunk Handling ---


          // --- Merge Logic (Only for non-chunked full_state messages, if any remain) ---
          // This path should ideally not be hit if chunking works correctly
          if (messageType === 'full_state' && parsed.entries) {
              this.log('MERGE_START_FULL_LEGACY', { peerId, entryCount: Object.keys(parsed.entries).length });
              const remoteDbState = {
                  entries: parsed.entries || {},
                  version: parsed.version,
                  lastModified: parsed.lastModified,
                  nodeId: peerId
              };
              await this.mergeReceivedState(remoteDbState, socket, parsed.syncChain);
          }
          // --- End Merge Logic ---

      } catch (err) {
         this.log('PEER_DATA_ERROR', {
              peerId: socket._peerNodeId || 'unknown', // Use socket's ID on error
              error: err.message,
              stack: err.stack,
              rawDataSample: data.toString('utf8').substring(0, 200)
          });
          this.stats.errors++;
          if (this.db) delete this.db.log;
          // Clear potentially corrupted chunk buffer on error
          if (socket) {
              socket._chunkBuffer = null;
              socket._syncMeta = null;
          }
      }
  }

  // *** NEW: Helper function to send full state in chunks ***
  async sendFullStateChunks(socket, trigger, syncChain) {
      const peerId = socket._peerNodeId || 'unknown';
      this.log('SENDING_FULL_STATE_CHUNKS', { peerId, trigger });

      try {
          const chunkSize = 1000; // Configurable chunk size
          let currentChunk = [];
          let sequence = 0;
          let totalEntries = 0;

          // Send sync_start marker (include current metadata)
          const startPayload = {
              type: 'sync_start',
              senderNodeId: this.nodeId,
              syncChain: syncChain,
              version: this.db.version,
              lastModified: this.db.lastModified
          };
          socket.write(JSON.stringify(startPayload));
          this.log('SYNC_CHUNK_SENT_START', { peerId, version: this.db.version });

          const iterator = this.db.db.iterator(); // Get LevelDB iterator
          for await (const [key, value] of iterator) {
              // Skip internal keys
              if (key === this.db.METADATA_VERSION_KEY || key === this.db.METADATA_LAST_MODIFIED_KEY || key === '__test__') continue;

              currentChunk.push({ key, value });
              totalEntries++;

              if (currentChunk.length >= chunkSize) {
                  sequence++;
                  const chunkPayload = {
                      type: 'sync_chunk',
                      senderNodeId: this.nodeId,
                      syncChain: syncChain,
                      sequence: sequence,
                      chunk: currentChunk
                  };
                  socket.write(JSON.stringify(chunkPayload));
                  // this.log('SYNC_CHUNK_SENT', { peerId, sequence, chunkSize: currentChunk.length }); // Reduce log verbosity
                  currentChunk = []; // Reset chunk
              }
          }
          // Ensure iterator is closed even if loop finishes early or throws
          await iterator.close().catch(err => console.error('Error closing iterator:', err));

          // Send any remaining entries in the last chunk
          if (currentChunk.length > 0) {
              sequence++;
              const chunkPayload = {
                  type: 'sync_chunk',
                  senderNodeId: this.nodeId,
                  syncChain: syncChain,
                  sequence: sequence,
                  chunk: currentChunk
              };
              socket.write(JSON.stringify(chunkPayload));
              // this.log('SYNC_CHUNK_SENT', { peerId, sequence, chunkSize: currentChunk.length }); // Reduce log verbosity
          }

          // Send sync_end marker
          const endPayload = {
              type: 'sync_end',
              senderNodeId: this.nodeId,
              syncChain: syncChain,
              totalEntries: totalEntries, // Send total count at the end
              version: this.db.version, // Send current version at end again for confirmation
              lastModified: this.db.lastModified // Send current mod time at end again
          };
          socket.write(JSON.stringify(endPayload));
          this.log('SYNC_CHUNK_SENT_END', { peerId, totalEntries, sequence });

      } catch (err) {
          this.log('SEND_CHUNKS_ERROR', { peerId, error: err.message, stack: err.stack });
          this.stats.errors++;
          // Optionally notify the peer of the error, e.g., send a sync_error message
          try {
              const errorPayload = { type: 'sync_error', senderNodeId: this.nodeId, error: 'Failed to send full state' };
              socket.write(JSON.stringify(errorPayload));
          } catch (writeErr) {
              // Ignore errors trying to send the error message
          }
      }
  }

  // *** NEW: Helper function to merge received state (called after reassembly or for direct full state) ***
  async mergeReceivedState(remoteDbState, socket, syncChain) {
      const peerId = remoteDbState.nodeId;
      try {
          // Ensure remoteDbState and entries are valid before proceeding
          if (!remoteDbState || typeof remoteDbState.entries !== 'object' || remoteDbState.entries === null) {
              this.log('MERGE_ABORTED', { peerId, reason: 'Invalid remote state or entries object' });
              return;
          }

          this.db.log = this.log.bind(this);
          let changes = 0;

          // Log before merge attempt
          this.log('MERGE_ATTEMPT', {
              peerId,
              remoteVersion: remoteDbState.version,
              remoteLastModified: remoteDbState.lastModified,
              remoteEntryCount: Object.keys(remoteDbState.entries).length,
              localVersion: this.db.version,
              localLastModified: this.db.lastModified
          });


          changes = await this.db.merge(remoteDbState);

          // Log after merge attempt
          this.log('MERGE_RESULT', {
              peerId,
              changes,
              newLocalVersion: this.db.version,
              newLocalLastModified: this.db.lastModified, // Log the updated local timestamp
              syncChain: syncChain
          });


          delete this.db.log; // Remove logger override

          if (changes > 0) {
              console.log(`Merged ${changes} changes from ${peerId}. New version: ${this.db.version}`);
              this.stats.changesMerged += changes;
              this.stats.lastChangeAt = new Date().toISOString();

              // Notify peers logic (Send metadata only)
              const otherPeers = Array.from(this.connections)
                  .filter(s => s !== socket && s && !s.destroyed); // Added check for s existence
              let peersToNotify = otherPeers;
              if (otherPeers.length > 3) {
                  peersToNotify = otherPeers.sort(() => Math.random() - 0.5).slice(0, 3);
              }

              this.log('NOTIFY_PEERS', {
                  sourcePeerId: peerId,
                  peerCount: peersToNotify.length,
                  totalPeers: otherPeers.length,
                  changes,
                  syncChain: syncChain
              });

              for (const otherSocket of peersToNotify) {
                  // Send metadata only, they will request full state if needed
                  await this.handleSyncRequest(otherSocket, `changes_from_${peerId}`);
              }
          }
      } catch (err) {
          this.log('MERGE_ERROR', { peerId, error: err.message, stack: err.stack });
          this.stats.errors++;
          if (this.db) delete this.db.log; // Ensure logger override is removed on error
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
      // await this.listAllEntries(); // REMOVED: Avoids loading all entries into memory at startup
      console.log(`Database initialized. Current version: ${this.db.version}, Last Modified: ${new Date(this.db.lastModified).toISOString()}`);

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
