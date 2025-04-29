#!/usr/bin/env node
'use strict';

const Hyperswarm = require('hyperswarm');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const { Level } = require('level');
// *** NEW: Import v8 module ***
const v8 = require('node:v8');
// *** END NEW ***

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

// *** NEW: Add signal handler for heap snapshots ***
let heapSnapshotCounter = 0;
process.on('SIGUSR2', () => {
  const filename = `heapdump-${process.pid}-${Date.now()}-${heapSnapshotCounter++}.heapsnapshot`;
  console.log(`Received SIGUSR2, writing heap snapshot to ${filename}...`);
  try {
    const generatedFilename = v8.writeHeapSnapshot(filename);
    console.log(`Heap snapshot written to ${generatedFilename}`);
  } catch (err) {
    console.error(`Error writing heap snapshot: ${err}`);
  }
});
// *** END NEW ***

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
    this.activeEntriesCount = 0; // << NEW: Initialize counter

    // Define keys for storing metadata within LevelDB
    this.METADATA_VERSION_KEY = '__metadata_version__';
    this.METADATA_LAST_MODIFIED_KEY = '__metadata_lastModified__';
    this.METADATA_ACTIVE_COUNT_KEY = '__metadata_active_count__'; // << NEW: Key for the counter
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
        // *** MODIFIED: Added cacheSize option ***
        const levelOptions = {
          valueEncoding: 'json',
          cacheSize: 256 * 1024 * 1024 // 256 MB cache size
        };
        this.db = new Level(this.dbDir, levelOptions);
        // *** END MODIFICATION ***

        await this.db.open();

        // *** Load or Initialize Metadata ***
        const initialMetadataBatch = this.db.batch();
        let metadataNeedsWrite = false;

        // Load/Init Version
        try {
          const storedVersion = await this.db.get(this.METADATA_VERSION_KEY);
          if (typeof storedVersion === 'number' && !isNaN(storedVersion)) {
            this.version = storedVersion;
            console.log(`Loaded existing DB version: ${this.version}`);
          } else {
            const originalValue = storedVersion;
            this.version = 0;
            console.warn(`Invalid stored version ('${originalValue}'). Defaulting to 0.`);
            initialMetadataBatch.put(this.METADATA_VERSION_KEY, this.version);
            metadataNeedsWrite = true;
          }
        } catch (err) {
          if (err.notFound) {
            console.log('No DB version found, starting at 0.');
            initialMetadataBatch.put(this.METADATA_VERSION_KEY, this.version); // Use initial value (0)
            metadataNeedsWrite = true;
          } else {
            console.error('Error loading DB version:', err);
            this.version = 0;
            console.warn(`Using version 0 due to load error.`);
            // Optionally write the fallback version? Maybe not, could overwrite valid data if read fails temporarily
          }
        }

        // Load/Init Last Modified
        try {
           const storedLastModified = await this.db.get(this.METADATA_LAST_MODIFIED_KEY);
           if (storedLastModified !== undefined && storedLastModified !== null && !isNaN(new Date(storedLastModified).getTime())) {
             this.lastModified = storedLastModified;
             console.log(`Loaded existing DB lastModified: ${new Date(this.lastModified).toISOString()}`);
           } else {
             const originalValue = storedLastModified;
             this.lastModified = Date.now();
             console.warn(`Invalid stored lastModified ('${originalValue}'). Defaulting to current time: ${new Date(this.lastModified).toISOString()}`);
             initialMetadataBatch.put(this.METADATA_LAST_MODIFIED_KEY, this.lastModified);
             metadataNeedsWrite = true;
           }
        } catch (err) {
             if (err.notFound) {
               console.log('No DB lastModified found, using current time.');
               initialMetadataBatch.put(this.METADATA_LAST_MODIFIED_KEY, this.lastModified); // Use initial value (Date.now())
               metadataNeedsWrite = true;
             } else {
               console.error('Error loading DB lastModified:', err);
               this.lastModified = Date.now();
               console.warn(`Using current time due to load error.`);
               // Optionally write fallback? See version comment.
             }
        }

        // << NEW: Load/Init Active Entry Count >>
        // *** MODIFIED: Always recalculate on startup ***
        try {
            console.log('Calculating active entry count on startup...');
            this.activeEntriesCount = await this._calculateActiveEntries(); // Always calculate
            console.log(`Calculated active count: ${this.activeEntriesCount}`);
            initialMetadataBatch.put(this.METADATA_ACTIVE_COUNT_KEY, this.activeEntriesCount); // Store the fresh count
            metadataNeedsWrite = true; // Mark metadata as needing write
        } catch (err) {
            console.error('Error calculating active entry count during startup:', err);
            this.activeEntriesCount = 0; // Fallback to 0 on calculation error
            console.warn(`Defaulting active count to 0 due to calculation error. Database might be inconsistent.`);
            // Optionally, still try to write 0? Or maybe prevent metadata write?
            // Let's still mark it for write, but the warning is important.
             initialMetadataBatch.put(this.METADATA_ACTIVE_COUNT_KEY, this.activeEntriesCount);
             metadataNeedsWrite = true;

        }
        // *** END MODIFICATION ***
        // << END NEW >>

        // Write initial/corrected metadata if needed
        if (metadataNeedsWrite) {
          await initialMetadataBatch.write();
          console.log('Wrote initial/corrected metadata values (including recalculated count).');
        }

        return; // Successfully opened and loaded metadata
      } catch (err) {
        retries--;
        if (err.code === 'LEVEL_LOCKED') {
          console.log(`Database is locked, retrying... (${retries} attempts left)`);
          await new Promise(resolve => setTimeout(resolve, 1000));
          continue;
        }
        // Close DB if open failed potentially mid-process
        if (this.db && this.db.status === 'open') {
             try { await this.db.close(); } catch (closeErr) { /* ignore */ }
             this.db = null;
        }
        throw err; // Rethrow other errors
      }
    }
    throw new Error('Failed to open database after multiple attempts');
  }

  // Helper method to get all entries (loads all into memory)
  async _getAllEntries() {
    if (!this.db) {
      throw new Error('Database not initialized');
    }

    const entries = {};
    let iterator;
    try {
      iterator = this.db.iterator();
      for await (const [key, value] of iterator) {
        // Skip internal metadata keys and test key
        if (key !== '__test__' && key !== this.METADATA_VERSION_KEY && key !== this.METADATA_LAST_MODIFIED_KEY && key !== this.METADATA_ACTIVE_COUNT_KEY) {
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

  // *** REMOVED: No longer needed for stats, recalculation handled in open() ***
  // async countActiveEntries() { ... }

  // << NEW: Internal helper for one-time calculation if count metadata is missing >>
  async _calculateActiveEntries() {
    if (!this.db) {
      console.warn('_calculateActiveEntries called before DB is open.');
      return 0; // Or throw?
    }
    let activeCount = 0;
    let iterator;
    try {
      iterator = this.db.iterator({ values: true }); // Need values to check deleted flag
      for await (const [key, value] of iterator) {
        // Skip internal metadata keys and test key
        if (key !== '__test__' &&
            key !== this.METADATA_VERSION_KEY &&
            key !== this.METADATA_LAST_MODIFIED_KEY &&
            key !== this.METADATA_ACTIVE_COUNT_KEY) { // Also skip count key
          // Check if the entry exists and is not marked as deleted
          if (value && !value.deleted) {
            activeCount++;
          }
        }
      }
    } catch (err) {
      console.error('Error calculating active entries:', err);
      return 0; // Return 0 on error during calculation
    } finally {
      if (iterator) {
        await iterator.close().catch(err => console.error('Error closing calculation iterator:', err));
      }
    }
    return activeCount;
  }

  // Helper to update metadata properties and persist to DB
  // << MODIFIED: Takes activeCountChange parameter >>
  async _updateMetadata(batch, activeCountChange = 0) {
      const newVersion = this.version + 1;
      const newLastModified = Date.now();
      const newActiveCount = this.activeEntriesCount + activeCountChange; // Calculate new count

      batch.put(this.METADATA_VERSION_KEY, newVersion);
      batch.put(this.METADATA_LAST_MODIFIED_KEY, newLastModified);
      batch.put(this.METADATA_ACTIVE_COUNT_KEY, newActiveCount); // Add count to batch

      // Return values to be set after the write succeeds.
      return { newVersion, newLastModified, newActiveCount };
  }

  // Add an entry
  async addEntry(infohashService, cacheStatus, expiration) {
    if (!this.db) {
      throw new Error('Database not initialized');
    }

    try {
        // << Check if entry already exists and is active >>
        const existingEntry = await this.db.get(infohashService).catch(() => null);
        const addingNewActiveEntry = !existingEntry || existingEntry.deleted; // It's new or was deleted

        const batch = this.db.batch();
        const newEntry = {
          cacheStatus,
          timestamp: new Date().toISOString(),
          expiration: expiration || new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(),
          updatedBy: this.nodeId,
          deleted: false
        };
        batch.put(infohashService, newEntry);

        // << Pass count change (+1 if adding new active, 0 otherwise) >>
        const countChange = addingNewActiveEntry ? 1 : 0;
        const { newVersion, newLastModified, newActiveCount } = await this._updateMetadata(batch, countChange);

        console.log(`[addEntry] Attempting to write version ${newVersion}, count ${newActiveCount} for key ${infohashService}`);
        await batch.write(); // Write entry and metadata atomically
        console.log(`[addEntry] Successfully wrote batch for key ${infohashService}`);

        this.version = newVersion;
        this.lastModified = newLastModified;
        this.activeEntriesCount = newActiveCount; // Update in-memory count

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
      if (!existingEntry) { // Don't update if non-existent
        return false;
      }

      const wasDeleted = existingEntry.deleted; // Check if it *was* deleted
      const isNowActive = true; // Updates always make it active (deleted: false)
      let countChange = 0;
      // If it was deleted and is now active, increment count
      if (wasDeleted && isNowActive) {
          countChange = 1;
      }
      // If it was already active and stays active, countChange remains 0.

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

      // Add metadata updates to the batch, including count change
      const { newVersion, newLastModified, newActiveCount } = await this._updateMetadata(batch, countChange);

      console.log(`[updateEntry] Attempting to write version ${newVersion}, count ${newActiveCount} for key ${infohashService}`);
      await batch.write(); // Write update and metadata atomically
      console.log(`[updateEntry] Successfully wrote batch for key ${infohashService}`);

      this.version = newVersion;
      this.lastModified = newLastModified;
      this.activeEntriesCount = newActiveCount; // Update in-memory count

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
      // Only proceed if the entry exists
      if (!existingEntry) {
        return false;
      }

      const wasActive = !existingEntry.deleted; // Check if it *was* active before this delete op
      let countChange = 0;
      // If it was active and is now being deleted, decrement count
      if (wasActive) {
          countChange = -1;
      }
      // If it was already deleted, countChange remains 0.

      // If countChange is 0 (was already deleted), we could potentially skip the write?
      // Let's write anyway to update timestamp/updatedBy, but log it.
      if (countChange === 0) {
          console.log(`[deleteEntry] Entry ${infohashService} was already deleted. Updating tombstone metadata.`);
      }

      const batch = this.db.batch();
      const tombstoneEntry = {
        ...existingEntry,
        deleted: true,
        timestamp: new Date().toISOString(),
        updatedBy: this.nodeId
      };
      batch.put(infohashService, tombstoneEntry); // Overwrite with tombstone

      // Add metadata updates to the batch, including count change
      const { newVersion, newLastModified, newActiveCount } = await this._updateMetadata(batch, countChange);

      console.log(`[deleteEntry] Attempting to write version ${newVersion}, count ${newActiveCount} for key ${infohashService}`);
      await batch.write(); // Write tombstone and metadata atomically
      console.log(`[deleteEntry] Successfully wrote batch for key ${infohashService}`);

      this.version = newVersion;
      this.lastModified = newLastModified;
      this.activeEntriesCount = newActiveCount; // Update in-memory count

      return true;
    } catch (err) {
      console.error(`[deleteEntry] Error writing batch for key ${infohashService}:`, err);
      return false;
    }
  }

  // Merge with another database - << Simplification: Let sync handler manage counts >>
  // The `merge` method is less critical now as direct merging from JSON/objects
  // is less common with the peer-to-peer sync. The core count logic will be
  // handled during sync chunk processing in P2PDBClient.handlePeerData.
  // We'll leave this method as is for now, acknowledging it won't update the
  // counter correctly if used directly.
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
            if (key === this.METADATA_VERSION_KEY || key === this.METADATA_LAST_MODIFIED_KEY || key === this.METADATA_ACTIVE_COUNT_KEY) continue;

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
    console.warn('[toJSON] Warning: Loading entire database into memory for JSON export.');
    const entries = await this._getAllEntries();
    // Filter out internal metadata keys from the entries export
    delete entries[this.METADATA_VERSION_KEY];
    delete entries[this.METADATA_LAST_MODIFIED_KEY];
    delete entries[this.METADATA_ACTIVE_COUNT_KEY]; // Also remove count key
    return {
      entries,
      version: this.version, // Use in-memory version
      lastModified: this.lastModified, // Use in-memory lastModified
      activeEntriesCount: this.activeEntriesCount, // Include current count
      nodeId: this.nodeId
    };
  }

  // Create from JSON - Less critical now, but ensure it writes metadata if used
  // << MODIFIED: Calculate and write count >>
  static async fromJSON(json, sourceNodeId) {
    const db = new P2PDatabase(sourceNodeId || json.nodeId || nodeId);
    try {
      // Open first, but ignore loaded metadata as JSON will overwrite it.
      // << MODIFIED: Make catch handler async >>
      await db.open().catch(async (err) => { // <<< Added async here
           console.warn(`Ignoring error during initial open in fromJSON as data will be overwritten: ${err.message}`);
           // Ensure db instance exists even if open failed before overwrite
           if (!db.db) {
               db.db = new Level(db.dbDir, { valueEncoding: 'json' });
               await db.db.open(); // <<< Now valid
           }
       });

      const entries = json.entries || {};
      const batch = db.db.batch();
      let entryCount = 0;
      let calculatedActiveCount = 0; // << Calculate count from JSON data

      for (const [key, value] of Object.entries(entries)) {
         // Skip internal metadata keys if present in the JSON entries
         if (key === db.METADATA_VERSION_KEY || key === db.METADATA_LAST_MODIFIED_KEY || key === db.METADATA_ACTIVE_COUNT_KEY) continue;
         if (key !== '__test__') {
             batch.put(key, value);
             entryCount++;
             // Calculate active count based on JSON data
             if (value && !value.deleted) {
                 calculatedActiveCount++;
             }
         }
      }

      // Use version/lastModified/count from JSON, overriding what open() might have loaded
      db.version = json.version || 0;
      db.lastModified = json.lastModified || Date.now();
      db.activeEntriesCount = json.activeEntriesCount !== undefined ? json.activeEntriesCount : calculatedActiveCount; // Use count from JSON if present, otherwise use calculated
      // If count wasn't in JSON, log the calculation.
      if (json.activeEntriesCount === undefined) {
           console.log(`Count not present in JSON, calculated ${db.activeEntriesCount} active entries.`);
      }


      // Write metadata from JSON (or calculated) to the DB as well
      batch.put(db.METADATA_VERSION_KEY, db.version);
      batch.put(db.METADATA_LAST_MODIFIED_KEY, db.lastModified);
      batch.put(db.METADATA_ACTIVE_COUNT_KEY, db.activeEntriesCount); // Write the count

      await batch.write(); // Write entries and metadata

      console.log(`Created DB from JSON. Wrote ${entryCount} entries. Version: ${db.version}. Active Count: ${db.activeEntriesCount}`);
      return db;
    } catch (err) {
      console.error(`[fromJSON] Error creating database from JSON:`, err);
      // Attempt to close if open
      if (db && db.db && db.db.status === 'open') {
           try { await db.close(); } catch (closeErr) { /* ignore */ }
      }
      throw err; // Rethrow error
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
    // *** MODIFIED: Increase syncCooldown ***
    this.syncCooldown = 6000; // 60 seconds minimum between syncs with the same peer
    // *** END MODIFICATION ***
    
    // Optimization 2: Version based sync - only sync if DB version changed
    this.peerVersions = new Map(); // Track last known DB version per peer
    
    // Setup memory usage tracking and cleanup
    this.memoryCheckInterval = setInterval(() => this.checkMemoryUsage(), 60000);
    // *** NEW: Map to hold processing state per socket ***
    this.syncProcessingState = new Map(); // Map socket -> { queue: [], processing: false }
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
    // << MODIFIED: Directly use the in-memory count >>
    const memoryUsage = process.memoryUsage();
    return {
      ...this.stats,
      connectionsActive: this.connections.size,
      databaseVersion: this.db.version,
      databaseEntries: this.db.activeEntriesCount, // << Use the maintained count
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
  // WARNING: HIGH MEMORY USAGE! This function loads the *entire* database
  // into memory via this.db.toJSON() before saving. Avoid calling it
  // frequently or on systems with tight memory constraints.
  // Consider streaming backups for very large databases.
  async saveDatabase() {
    try {
      console.warn('[saveDatabase] Warning: Loading entire database into memory for snapshot...');
      const dbJson = await this.db.toJSON(); // This loads all entries into memory!
      const data = JSON.stringify(dbJson, null, 2);
      const filePath = this.getSaveFilePath();
      fs.writeFileSync(filePath, data);
      console.log('Saved database snapshot to disk:', filePath);
    } catch (err) {
        console.error('Error saving database snapshot:', err);
    }
  }
  
  // Function to list all entries
  // WARNING: This function iterates and logs all non-metadata entries.
  // Avoid using in production if value sizes are large or entry count is massive.
  async listAllEntries() {
    console.log(`Listing all entries (Version: ${this.db.version}, Last Modified: ${new Date(this.db.lastModified).toISOString()})...`);
    let count = 0;
    const iterator = this.db.db.iterator();
    try {
        for await (const [key, value] of iterator) {
            // Skip internal metadata keys and test key
            if (key !== '__test__' && key !== this.db.METADATA_VERSION_KEY && key !== this.db.METADATA_LAST_MODIFIED_KEY && key !== this.db.METADATA_ACTIVE_COUNT_KEY) {
                // Optional: Log only active entries
                // if (value && !value.deleted) {
                //     console.log(`${key}:`, value);
                //     count++;
                // }
                // Or log all non-metadata entries:
                console.log(`${key}:`, value);
                count++;
            }
        }
        console.log(`Finished listing ${count} non-metadata entries.`);
    } catch (err) {
        console.error('Error iterating through entries for listing:', err);
    } finally {
        await iterator.close().catch(err => console.error('Error closing list iterator:', err));
    }
    // Original implementation (loads all to memory):
    // const entries = await this.db._getAllEntries();
    // console.log('Last modified:', new Date(this.db.lastModified).toISOString());
    // return entries;
  }
  
  // *** NEW: Helper function to write data with backpressure handling ***
  _writeDataWithBackpressure(socket, data, logContext) {
    return new Promise((resolve, reject) => {
        // *** MODIFIED: Append newline delimiter ***
        const dataWithNewline = data + '\n';
        // *** END MODIFICATION ***

        // *** MODIFIED: Use dataWithNewline ***
        const writeSuccessful = socket.write(dataWithNewline, (err) => {
            // *** END MODIFICATION ***
        if (err) {
          this.log('WRITE_CALLBACK_ERROR', { ...logContext, error: err.message });
          // It's safer to perhaps not reject here, as the main error/close handlers
          // might handle the socket destruction and rejection more gracefully.
          // Let's remove the explicit reject from the callback.
        }
      });

      if (writeSuccessful) {
        resolve();
              } else {
        this.log('WRITE_BUFFER_FULL_WAITING_DRAIN', { ...logContext });
        socket.once('drain', () => {
          this.log('WRITE_DRAIN_EVENT_RECEIVED', { ...logContext });
          resolve();
        });
      }
    });
  }


  // *** MODIFIED: Use helper function in handleSyncRequest ***
  async handleSyncRequest(socket, trigger = 'unknown', parentSyncChain = []) { // Added parentSyncChain param
    const peerIdForLookup = socket._peerNodeId || 'unknown';
    const logContext = { peerId: peerIdForLookup, trigger, connectionId: socket._connectionId || 'N/A' };

    try {
      // Cooldown check
      const now = Date.now();
      const lastSyncTime = this.lastSyncTimes.get(peerIdForLookup) || 0;
      if (trigger !== 'initial_connection' && now - lastSyncTime < this.syncCooldown) {
        this.log('SYNC_SKIPPED', { ...logContext, reason: 'cooldown', timeSinceLastSync: now - lastSyncTime });
        return;
      }

      // *** REMOVED: Loop detection check is moved to handlePeerData on receive ***
      // const syncChain = socket._syncChain || []; // OLD
      // if (syncChain.includes(this.nodeId)) { ... } // OLD

      // *** REVISED: Construct chain for sending based on trigger/parent ***
      // For requests initiated locally (initial, periodic, local_change), start a fresh chain.
      // If responding to a request, build upon the parent chain.
      const chainToSend = [...parentSyncChain, this.nodeId];
      // socket._syncChain = chainToSend; // OLD: Don't store on socket

      // --- Send metadata ---
      const currentVersion = this.db.version;
      const currentLastModified = this.db.lastModified;
      const messageType = 'metadata_only';

      this.log('SYNC_PREPARING_METADATA', { ...logContext, localVersion: currentVersion });

      const messagePayload = {
        type: messageType,
        senderNodeId: this.nodeId,
        syncChain: chainToSend, // *** Use constructed chainToSend ***
        version: currentVersion,
        lastModified: currentLastModified,
      };

      this.log('SYNC_START', { ...logContext, messageType, syncChain: chainToSend, activeConnections: this.connections.size });

      const message = JSON.stringify(messagePayload);
      await this._writeDataWithBackpressure(socket, message, { ...logContext, type: 'metadata_only' });

      this.stats.syncsSent++;
      this.stats.lastSyncAt = new Date().toISOString();
      this.lastSyncTimes.set(peerIdForLookup, Date.now());

      this.log('SYNC_SENT', { ...logContext, messageType, dataSize: message.length, syncChain: chainToSend, version: currentVersion });

    } catch (err) {
      this.log('SYNC_ERROR', { ...logContext, error: err.message, stack: err.stack });
      this.stats.errors++;
    }
  }

  // Handle incoming data from peers (Handles Metadata, Requests, and Chunks)
  // *** MODIFIED: Accept parsed object directly, remove raw data and JSON.parse ***
  async handlePeerData(socket, _, parsed) {
      let currentPeerId = socket._peerNodeId || 'unknown';
      let peerId; // Define peerId outside try block

      try {
          const messageSenderId = parsed.senderNodeId;
          peerId = currentPeerId; // Assign initial value

          // --- Peer ID Handling ---
          if (messageSenderId) { // <--- Check if senderNodeId exists in the message
              peerId = messageSenderId;
              // *** NEW: Add diagnostic log before the check ***
              this.log('PEER_ID_CHECK_STATE', {
                  connectionId: socket._connectionId || 'N/A',
                  currentPeerIdOnSocket: socket._peerNodeId, // Value directly from socket property
                  currentPeerIdVar: currentPeerId,         // Value of the variable in this function scope
                  messageSenderId: messageSenderId,        // The ID from the incoming message
                  conditionResult: messageSenderId !== currentPeerId // Explicitly log the condition outcome
              });
              // *** END NEW ***
              if (messageSenderId !== currentPeerId) {
                  socket._peerNodeId = messageSenderId; // <--- The assignment happens here
                  this.log('PEER_ID_ASSOCIATED', { connectionId: socket._connectionId || 'N/A', assignedPeerId: messageSenderId });
                  this.log('PEER_ID_UPDATED', { connectionId: socket._connectionId || 'N/A', oldId: currentPeerId, newId: messageSenderId });
                  if (currentPeerId !== 'unknown') {
                     this.lastSyncTimes.delete(currentPeerId);
                     this.peerVersions.delete(currentPeerId);
                  }
                  currentPeerId = messageSenderId; // Update currentPeerId after logging
              }
          } else if (peerId === 'unknown') { // <--- If senderNodeId is MISSING
              this.log('PEER_DATA_WARNING', { connectionId: socket._connectionId || 'N/A', message: "Received data without senderNodeId from unidentified peer. Ignoring.", parsedDataSample: JSON.stringify(parsed).substring(0, 200) });
              return;
          }
          // --- End Peer ID Handling ---

          // *** NEW: Loop detection check on RECEIVE ***
          // const incomingSyncChain = parsed.syncChain || [];
          // if (incomingSyncChain.includes(this.nodeId)) {
          //     this.log('SYNC_LOOP_DETECTED_ON_RECEIVE', {
          //         connectionId: socket._connectionId || 'N/A',
          //         peerId,
          //         receivedChain: incomingSyncChain,
          //         messageTypeHint: parsed.type || 'unknown' // Add hint about message type being dropped
          //     });
          //     return; // Ignore message due to loop
          // }
          // *** END NEW ***

          // socket._syncChain = incomingSyncChain; // OLD: Don't store on socket anymore
          this.stats.syncsReceived++;
          const messageType = parsed.type || (parsed.requestSync ? 'sync_request' : (parsed.sequence ? 'sync_chunk' : (parsed.chunk ? 'sync_chunk' : (parsed.totalEntries !== undefined ? 'sync_end' : (parsed.version !== undefined ? 'metadata_only' : 'unknown')))));

          // --- Handle Sync Requests ---
          if (messageType === 'sync_request') {
              this.log('HANDLE_PEER_DATA_BRANCH', { connectionId: socket._connectionId || 'N/A', peerId, branch: 'sync_request' }); // <-- Add logging
              const remoteVersion = parsed.version; // Peer's version when requesting
              const logContext = { connectionId: socket._connectionId || 'N/A', peerId, localVersion: this.db.version, remoteVersion };

              this.log('SYNC_REQUEST_RECEIVED', logContext);

              // Decide whether to send full state (e.g., maybe check version again, or just comply)
              // Let's assume we always comply with a direct request for now.
              this.log('SENDING_FULL_STATE_ON_REQUEST', logContext);

              // *** Pass the incomingSyncChain to sendFullStateChunks ***
              this.sendFullStateChunks(socket, 'sync_request_response', incomingSyncChain)
                  .then(() => {
                      this.log('FULL_STATE_SENT_COMPLETE', logContext);
                  })
                  .catch(err => {
                      this.log('FULL_STATE_SEND_ERROR', { ...logContext, error: err.message });
                  });
              return; // Return after initiating send
          }

          // --- Handle Metadata & Potentially Request Sync ---
          if (parsed.version !== undefined) {
              this.peerVersions.set(peerId, parsed.version);
          }
          if (messageType === 'metadata_only') {
              this.log('HANDLE_PEER_DATA_BRANCH', { connectionId: socket._connectionId || 'N/A', peerId, branch: 'metadata_only' }); // <-- Add logging
              const remoteVersion = parsed.version;
              const remoteLastModified = parsed.lastModified;
              const localVersion = this.db.version;
              const localLastModified = this.db.lastModified;

              // *** NEW: Log received metadata BEFORE comparison ***
              this.log('RECEIVED_METADATA_FROM_PEER', {
                  connectionId: socket._connectionId || 'N/A',
                  peerId,
                  remoteVersion, remoteLastModified,
                  localVersion, localLastModified
              });

              if ((remoteVersion > localVersion || (remoteLastModified && localLastModified && Date.parse(remoteLastModified) > Date.parse(localLastModified)))) {
                  const now = Date.now();
                  const lastSyncReqTime = socket._lastSyncRequestTime || 0;
                  const syncRequestCooldown = 60000; // <-- INCREASED TO 60 SECONDS
                  if (now - lastSyncReqTime > syncRequestCooldown) {
                      const logContext = { connectionId: socket._connectionId || 'N/A', peerId, localVersion, remoteVersion, remoteLastModified, localLastModified };
                      this.log('REQUESTING_SYNC_FROM_PEER', logContext);
                      const requestPayload = {
                          type: 'sync_request',
                          requestType: 'full_state',
                          senderNodeId: this.nodeId,
                          version: localVersion, // Include our version
                          syncChain: [...(parsed.syncChain || []), this.nodeId]
                      };
                      const requestMessage = JSON.stringify(requestPayload);
                      await this._writeDataWithBackpressure(socket, requestMessage, { ...logContext, type: 'sync_request' });
                      this.log('SYNC_REQUEST_SENT_SUCCESSFULLY', { ...logContext });
                      socket._lastSyncRequestTime = now; // Track when we last requested
                      return; // Return after requesting sync
                  } else {
                      this.log('REQUEST_SYNC_SKIPPED', { connectionId: socket._connectionId || 'N/A', peerId, reason: 'cooldown', timeSinceLastReq: now - lastSyncReqTime });
                  }
              }
              // If we don't need to request sync back, we just received metadata, potentially updated peerVersion map.
              // No further action needed for metadata_only here.
              return; // Explicitly return after handling metadata_only
          }
          // --- End Metadata & Request Logic ---

          // --- Handle Sync Start, Chunk, End ---
          const BATCH_WRITE_THRESHOLD = 1000; // <-- INCREASED THRESHOLD

          if (messageType === 'sync_start') {
              this.log('HANDLE_PEER_DATA_BRANCH', { connectionId: socket._connectionId || 'N/A', peerId, branch: 'sync_start' }); // <-- Add logging
              if (socket._syncBatch) {
                 this.log('SYNC_CHUNK_WARNING_BATCHING', { connectionId: socket._connectionId || 'N/A', peerId, error: 'Received sync_start while already in a sync batch state. Ignoring new start.'});
                 return;
              }
              socket._syncBatch = this.db.db.batch();
              socket._syncProcessedCounter = 0;
              socket._syncTotalWrittenCounter = 0;
              socket._syncInitialLocalVersion = this.db.version;
              socket._syncInitialLocalLastModified = this.db.lastModified;
              socket._syncInitialLocalCount = this.db.activeEntriesCount; // *** Store initial count ***
              socket._syncNetCountChange = 0; // *** Initialize net change counter ***
              socket._syncRemoteMeta = { version: parsed.version, lastModified: parsed.lastModified };
              socket._activeChunkHandlers = 0; // Reset active handlers counter
              socket._syncReadyToClean = false;

              // *** Store the incoming chain associated with this sync operation ***
              socket._currentSyncChain = incomingSyncChain;
              this.log('SYNC_CHUNK_START_BATCHING', {
                  connectionId: socket._connectionId || 'N/A',
                  peerId,
                  remoteVersion: parsed.version,
                  remoteLastModified: parsed.lastModified,
                  initialLocalVersion: socket._syncInitialLocalVersion,
                  initialLocalLastModified: socket._syncInitialLocalLastModified,
                  initialLocalCount: this.db.activeEntriesCount,
                  receivedChain: incomingSyncChain // Log the chain starting this sync
              });
              return; // Wait for chunks
          }

          if (messageType === 'sync_chunk') {
              this.log('HANDLE_PEER_DATA_BRANCH', { connectionId: socket._connectionId || 'N/A', peerId, branch: 'sync_chunk', sequence: parsed.sequence });
              let currentBatchForChunk = socket._syncBatch;
              if (!currentBatchForChunk) {
                  this.log('SYNC_CHUNK_ERROR_MISSING_BATCH', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence });
                   return;
              }

              const chunkEntries = parsed.chunk || [];
              if (parsed.sequence % 10 === 1) { this.log('SYNC_CHUNK_RECEIVED_BATCHING', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, chunkSize: chunkEntries.length }); }

              socket._activeChunkHandlers++;
              let processingErrorOccurred = false;
              try {
                  // *** BATCH READ OPTIMIZATION ***
                  const keysToFetch = chunkEntries
                      .map(entry => entry?.key)
                      .filter(key => key && key !== this.db.METADATA_VERSION_KEY && key !== this.db.METADATA_LAST_MODIFIED_KEY && key !== this.db.METADATA_ACTIVE_COUNT_KEY && key !== '__test__'); // Filter out invalid/internal keys

                  this.log('SYNC_CHUNK_GETMANY_START', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, keyCount: keysToFetch.length });
                  const fetchStartTime = Date.now();
                  const localEntriesArray = await this.db.db.getMany(keysToFetch).catch(err => {
                       this.log('SYNC_CHUNK_GETMANY_ERROR', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, error: err.message });
                       processingErrorOccurred = true; // Mark error if getMany fails
                       return []; // Return empty array on error to stop processing
                  });
                  const fetchDuration = Date.now() - fetchStartTime;
                  this.log('SYNC_CHUNK_GETMANY_COMPLETE', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, resultsCount: localEntriesArray.length, durationMs: fetchDuration });

                  // Convert array to map for fast lookup (handle potential undefined values from getMany if key not found)
                  const localEntriesMap = new Map(keysToFetch.map((key, index) => [key, localEntriesArray[index]]));
                  // *** END BATCH READ OPTIMIZATION ***

                  if (processingErrorOccurred) { // Exit early if getMany failed
                     throw new Error('getMany failed during chunk processing.');
                  }

                  for (const entry of chunkEntries) {
                      if (processingErrorOccurred) break;
                      // Skip invalid/internal entries (already filtered for getMany, but double-check here)
                      if (!entry || entry.key === undefined || entry.value === undefined ||
                          entry.key === this.db.METADATA_VERSION_KEY ||
                          entry.key === this.db.METADATA_LAST_MODIFIED_KEY ||
                          entry.key === this.db.METADATA_ACTIVE_COUNT_KEY ||
                          entry.key === '__test__') {
                           continue;
                      }

                      try {
                          // *** Use the map for local entry lookup ***
                          const localEntry = localEntriesMap.get(entry.key);
                          // *** (Remove the 'await this.db.db.get()' call) ***

                          // --- Concurrency Checks (Still useful) ---
                          if (!socket._syncBatch || socket._syncBatch !== currentBatchForChunk) {
                               this.log('SYNC_CHUNK_WARNING_BATCHING (Inside Loop)', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, message: 'Batch invalid concurrently during entry processing.', entryKey: entry.key });
                               processingErrorOccurred = true; break;
                          }
                          // ---

                          const remoteEntry = entry.value;
                          let shouldUpdate = false;
                          // *** MODIFICATION START: Use string comparison ***
                          // let localTimestampParsed = NaN; // No longer needed
                          // let remoteTimestampParsed = NaN; // No longer needed

                          // Note: localEntry will be undefined if key wasn't found by getMany
                          if (localEntry === undefined) {
                              shouldUpdate = true;
                              // No parsing needed here either
                          } else if (localEntry.timestamp && remoteEntry?.timestamp) { // Check both exist
                              // Directly compare ISO timestamp strings
                              shouldUpdate = localEntry.timestamp < remoteEntry.timestamp;
                              // Removed Date.parse calls
                          }
                          // *** MODIFICATION END ***

                          // Log comparison (can be verbose)
                           // *** MODIFICATION START: Update logging if desired (optional) ***
                           this.log('SYNC_CHUNK_COMPARE_TIMESTAMPS', {
                               connectionId: socket._connectionId || 'N/A',
                               peerId, sequence: parsed.sequence,
                               entryKey: entry.key,
                               localTs: localEntry?.timestamp, // Keep original strings
                               remoteTs: remoteEntry?.timestamp, // Keep original strings
                               // localParsed: localTimestampParsed, // Remove parsed values
                               // remoteParsed: remoteTimestampParsed, // Remove parsed values
                               shouldUpdate
                            });
                           // *** MODIFICATION END ***

                          if (shouldUpdate) {
                              this.log('SYNC_CHUNK_PERFORMING_UPDATE', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, entryKey: entry.key });
                              let countChange = 0;
                              const remoteIsActive = remoteEntry && !remoteEntry.deleted;
                              const localWasActive = localEntry !== undefined && !localEntry.deleted; // Check localEntry is not undefined

                              if (remoteIsActive && !localWasActive) countChange = 1;
                              else if (!remoteIsActive && localWasActive) countChange = -1;

                              try {
                                  currentBatchForChunk.put(entry.key, remoteEntry);
                                  socket._syncProcessedCounter++;
                                  socket._syncTotalWrittenCounter++;
                                  socket._syncNetCountChange += countChange;
                              } catch (putErr) {
                                   this.log('SYNC_CHUNK_PUT_ERROR', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, key: entry.key, error: putErr.message });
                                   processingErrorOccurred = true; break;
                              }

                              // --- Intermediate Write Check (using new threshold) ---
                              if (socket._syncProcessedCounter >= BATCH_WRITE_THRESHOLD) {
                                  const batchToWrite = currentBatchForChunk;
                                  this.log('SYNC_CHUNK_WRITING_INTERMEDIATE_BATCH', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, batchSize: batchToWrite.length, threshold: BATCH_WRITE_THRESHOLD });
                                  try {
                                      const writeStartTime = Date.now();
                                      await batchToWrite.write();
                                      const writeDuration = Date.now() - writeStartTime;
                                      this.log('SYNC_CHUNK_INTERMEDIATE_BATCH_SUCCESS', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, batchSize: batchToWrite.length, durationMs: writeDuration });

                                      socket._syncBatch = this.db.db.batch();
                                      currentBatchForChunk = socket._syncBatch;
                                      socket._syncProcessedCounter = 0;

                                  } catch (writeErr) {
                                       this.log('SYNC_CHUNK_INTERMEDIATE_BATCH_ERROR', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, error: writeErr.message });
                                       processingErrorOccurred = true; break;
                                  }
                              } // --- End Intermediate Write ---
                          } // End shouldUpdate
                      } catch (entryProcessingErr) {
                           this.log('SYNC_CHUNK_ENTRY_PROCESSING_ERROR', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, entryKey: entry?.key, error: entryProcessingErr.message, stack: entryProcessingErr.stack });
                           processingErrorOccurred = true; break;
                      }
                  } // End for loop
              } finally {
                  socket._activeChunkHandlers--;
                  if (processingErrorOccurred) {
                      this.log('SYNC_CHUNK_PROCESSING_ERROR_FLAG_SET', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence });
                  }
                  if (socket._activeChunkHandlers === 0 && socket._syncReadyToClean) {
                      this.log('SYNC_CHUNK_PERFORMING_DEFERRED_CLEANUP', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence });
                     this._cleanupSyncState(socket);
                  }
              }
          } // End sync_chunk

          if (messageType === 'sync_end') {
              this.log('HANDLE_PEER_DATA_BRANCH', { connectionId: socket._connectionId || 'N/A', peerId, branch: 'sync_end' }); // <-- Add logging
              let finalBatch = socket._syncBatch;
              if (!finalBatch) {
                  // *** NEW: Add specific log for missing batch ***
                  this.log('SYNC_END_ERROR_MISSING_BATCH', { connectionId: socket._connectionId || 'N/A', peerId, totalEntries: parsed.totalEntries });
                  // Consider cleanup? Or just return? Let's return for now.
                   return;
                  // *** END NEW ***
              }

              this.log('SYNC_CHUNK_END_BATCHING', { /* ... */ });
              let changesMade = false;
              // *** Store the calculated net change BEFORE potential errors/cleanup ***
              const finalNetCountChange = socket._syncNetCountChange || 0;

              try {
                  // Write final batch
                  if (finalBatch.length > 0) {
                      // ... log writing final ...
                      await finalBatch.write();
                      // ... log success ...
                      changesMade = true;
                  } else { /* ... log empty ... */ }

                  // Verify total entry count
                  if (socket._syncTotalWrittenCounter !== parsed.totalEntries) { /* ... log mismatch ... */ }
                  else { /* ... log match ... */ }

                  // --- Update Local Metadata ---
                  const remoteVersion = parsed.version !== undefined ? parsed.version : socket._syncRemoteMeta?.version;
                  const remoteLastModified = parsed.lastModified !== undefined ? parsed.lastModified : socket._syncRemoteMeta?.lastModified;
                  const initialLocalVersion = socket._syncInitialLocalVersion;
                  const initialLocalLastModified = socket._syncInitialLocalLastModified;
                  const initialLocalCount = socket._syncInitialLocalCount; // Get stored initial count
                  const remoteStateWasNewer = (remoteVersion > initialLocalVersion || (remoteLastModified && initialLocalLastModified && Date.parse(remoteLastModified) > Date.parse(initialLocalLastModified)));

                  if (changesMade || remoteStateWasNewer) {
                     const metadataBatch = this.db.db.batch();
                     const entriesWereWritten = socket._syncTotalWrittenCounter > 0;
                     const shouldIncrementVersion = entriesWereWritten || (remoteStateWasNewer && !entriesWereWritten);
                     const newVersion = Math.max(initialLocalVersion, remoteVersion || 0) + (shouldIncrementVersion ? 1 : 0);
                     const newLastModified = Date.now();

                     // *** Use incremental count update ***
                     const newActiveCount = initialLocalCount + finalNetCountChange;
                     this.log('SYNC_CHUNK_UPDATING_COUNT', { connectionId: socket._connectionId || 'N/A', peerId, initialCount: initialLocalCount, netChange: finalNetCountChange, newCount: newActiveCount });
                     // *** REMOVED: _calculateActiveEntries() call ***

                     metadataBatch.put(this.db.METADATA_VERSION_KEY, newVersion);
                     metadataBatch.put(this.db.METADATA_LAST_MODIFIED_KEY, newLastModified);
                     metadataBatch.put(this.db.METADATA_ACTIVE_COUNT_KEY, newActiveCount); // Write the new count

                     this.log('SYNC_CHUNK_WRITING_METADATA', { /* ... include newCount ... */ });
                     await metadataBatch.write();

                     // Update in-memory state
                     this.db.version = newVersion;
                     this.db.lastModified = newLastModified;
                     this.db.activeEntriesCount = newActiveCount; // *** Update memory count ***

                     this.log('SYNC_CHUNK_METADATA_UPDATED', { /* ... include newActiveCount ... */});
                     changesMade = true;
                  } else { /* ... log no update ... */ }

                  // --- Notify Peers ---
                  if (changesMade) {
                      console.log(`Batch sync completed from ${peerId} (Conn: ${socket._connectionId || 'N/A'}). ${socket._syncTotalWrittenCounter || 0} entries processed. New version: ${this.db.version}. New Count: ${this.db.activeEntriesCount}.`); // Log updated count
                      this.stats.changesMerged += socket._syncTotalWrittenCounter || 0;
                      this.stats.lastChangeAt = new Date().toISOString();
                      // *** CHANGE 3: Use the stored chain from the sync operation ***
                      const propagationChain = socket._currentSyncChain || []; // Use stored chain
                      this.syncWithPeers(`sync_complete_from_${peerId}`, propagationChain); // Pass the correct chain
                  } else { /* ... log no changes applied ... */ }

              } catch (err) {
                  // *** NEW: Added detailed logging for final processing errors ***
                  this.log('SYNC_CHUNK_FINAL_PROCESSING_ERROR', {
                      connectionId: socket._connectionId || 'N/A',
                      peerId,
                      error: err.message, stack: err.stack
                  });
               }
               finally {
                   // Mark ready for cleanup, actual cleanup might be deferred
                   socket._syncReadyToClean = true;
                   // If no chunk handlers are active, clean up immediately
                   if (socket._activeChunkHandlers === 0) {
                      this.log('SYNC_CHUNK_PERFORMING_IMMEDIATE_CLEANUP_ON_END', { connectionId: socket._connectionId || 'N/A', peerId });
                      this._cleanupSyncState(socket);
                   } else {
                      this.log('SYNC_CHUNK_DEFERRING_CLEANUP_ON_END', { connectionId: socket._connectionId || 'N/A', peerId, activeHandlers: socket._activeChunkHandlers });
                   }
               }
              return; // Sync complete
          }
          // --- End Sync Start, Chunk, End ---

          // *** NEW: Add log if message type was not handled ***
          if (!['sync_request', 'metadata_only', 'sync_start', 'sync_chunk', 'sync_end'].includes(messageType)) {
               this.log('HANDLE_PEER_DATA_UNHANDLED_TYPE', { connectionId: socket._connectionId || 'N/A', peerId, messageType, parsedKeys: Object.keys(parsed) });
          }
          // *** END NEW ***

      } catch (err) {
          // Ensure peerId is defined for logging
         const errorPeerId = socket?._peerNodeId || currentPeerId || 'unknown';
          // ... rest of general error handling ...
      }
  }

  // *** NEW: Helper function to send full state in chunks ***
  async sendFullStateChunks(socket, trigger, parentSyncChain = []) { // Added parentSyncChain default
      const peerId = socket._peerNodeId || 'unknown';
      // *** Construct chain to SEND ***
      const chainToSend = [...parentSyncChain, this.nodeId];
      const logContext = { peerId, trigger, connectionId: socket._connectionId || 'N/A', syncChain: chainToSend }; // Include chain in context

      this.log('SENDING_FULL_STATE_CHUNKS (Load First)', { ...logContext });

      let allEntries = [];
      try {
          // --- Step 1: Load all relevant data from DB ---
          // ... (loading logic unchanged) ...

          // --- Step 2: Send data in chunks with backpressure ---
          const chunkSize = 100;
          let sequence = 0;
          const totalEntries = allEntries.length;

          // Send sync_start marker
          const startPayload = {
              type: 'sync_start',
              senderNodeId: this.nodeId,
              syncChain: chainToSend, // *** Use chainToSend ***
              version: this.db.version,
              lastModified: this.db.lastModified
          };
          const startMessage = JSON.stringify(startPayload);
          await this._writeDataWithBackpressure(socket, startMessage, { ...logContext, type: 'sync_start' });
          this.log('SYNC_CHUNK_SENT_START', { ...logContext, version: this.db.version }); // Log context includes chain

          // Loop through the in-memory array
          for (let i = 0; i < totalEntries; i += chunkSize) {
              sequence++;
              const chunk = allEntries.slice(i, i + chunkSize);
              const chunkPayload = {
                  type: 'sync_chunk',
                  senderNodeId: this.nodeId,
                  syncChain: chainToSend, // *** Use chainToSend ***
                  sequence,
                  chunk
              };
              const chunkMessage = JSON.stringify(chunkPayload);
              await this._writeDataWithBackpressure(socket, chunkMessage, { ...logContext, type: 'sync_chunk', sequence });
              if (sequence % 10 === 1) {
                 this.log('SEND_CHUNK_PROGRESS (Load First)', { ...logContext, sequence, sent: i + chunk.length, total: totalEntries });
              }
          }

          this.log('SEND_CHUNK_CLEARING_ARRAY', { ...logContext, sizeBeforeClear: allEntries.length });
          allEntries = [];

          // Send sync_end marker
          const endPayload = {
              type: 'sync_end',
              senderNodeId: this.nodeId,
              syncChain: chainToSend, // *** Use chainToSend ***
              totalEntries: totalEntries,
              version: this.db.version,
              lastModified: this.db.lastModified
          };
          const endMessage = JSON.stringify(endPayload);
          await this._writeDataWithBackpressure(socket, endMessage, { ...logContext, type: 'sync_end' });
          this.log('SYNC_CHUNK_SENT_END', { ...logContext, totalEntries, sequence }); // Log context includes chain

      } catch (err) {
          this.log('SEND_CHUNK_CLEARING_ARRAY_ON_ERROR', { ...logContext, sizeBeforeClear: allEntries?.length || 0 });
          allEntries = [];
          this.log('SEND_CHUNKS_ERROR (Load First)', { ...logContext, error: err.message, stack: err.stack }); // Log context includes chain
          this.stats.errors++;
          try {
              // Send error message - include the chain that failed? Maybe not necessary.
              const errorPayload = { type: 'sync_error', senderNodeId: this.nodeId, error: 'Failed to send full state' };
              socket.write(JSON.stringify(errorPayload));
          } catch (writeErr) { /* Ignore */ }
      }
  }

  // *** REMOVED: mergeReceivedState function is no longer needed ***


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
        let connectionId = null; // Define here
        try { // Added try block around the entire connection setup
          connectionId = `conn_${Date.now().toString(36)}_${Math.random().toString(36).substring(2, 7)}`;
          socket._connectionId = connectionId;

          this.connections.add(socket);
          this.stats.connectionsTotal++;

          // *** NEW: Initialize sync queue state for this socket ***
          this.syncProcessingState.set(socket, { queue: [], processing: false });

          this.log('NEW_CONNECTION', {
            connectionId,
            peerId: 'unknown', // Always unknown initially here
            remoteAddress: info?.remoteAddress,
            type: info?.type
          });

          // Initial sync - share our database state
          // *** Call handleSyncRequest WITHOUT a parent chain (starts fresh) ***
          this.handleSyncRequest(socket, 'initial_connection').catch(err => {
            this.log('SYNC_ERROR_INITIAL', { connectionId, error: err.message, stack: err.stack });
            this.stats.errors++;
          });

          // Handle incoming data from peers
          let buffer = '';
          // REMOVED: let peerNodeId = null;
          const MAX_BUFFER_SIZE = 50 * 1024 * 1024;

          socket.on('data', data => {
              const connectionId = socket._connectionId || 'unknown';
              let peerId = socket._peerNodeId || 'unknown'; // Use known peer ID if available

              buffer += data.toString('utf8');

              // *** Log the raw data chunk received - KEEP for debugging ***
              this.log('SOCKET_RAW_DATA_RECEIVED', {
                  connectionId,
                  peerId,
                  rawDataChunk: data.toString('utf8')
              });

              // Buffer limit check - KEEP
              if (buffer.length > MAX_BUFFER_SIZE) {
                  // ... (buffer limit logic unchanged) ...
                  return;
              }

              // *** More Detailed Logging in Processing Logic ***
              (async () => { // <<< START OF ASYNC IIFE
                  this.log('SOCKET_DATA_HANDLER_ENTERED', { connectionId, peerId, bufferLength: buffer.length }); // <<< Log entry

                  let potentialJson = buffer;
                  let parsedObject = null;

                  if (potentialJson.trim().length > 0) {
                      this.log('SOCKET_BUFFER_HAS_CONTENT', { connectionId, peerId });
                      try {
                           this.log('SOCKET_ATTEMPTING_PARSE', { /* ... */ });
                           parsedObject = JSON.parse(potentialJson);
                           const parsedMessageSize = Buffer.byteLength(potentialJson, 'utf8');

                           // If parse succeeded:
                           const consumedBuffer = buffer; // Store buffer before clearing
                           buffer = ''; // Clear buffer now that parse is successful
                           this.log('SOCKET_JSON_PARSE_SUCCESS', {
                               connectionId,
                               peerId: socket._peerNodeId || parsedObject?.senderNodeId || peerId, // Log the ID we found
                               consumedBufferLength: consumedBuffer.length // Use the stored length
                           });

                           // *** MOVED PROCESSING LOGIC HERE (INSIDE TRY, AFTER SUCCESS) ***
                           this.log('SOCKET_PARSED_OBJECT_PREVIEW', {
                               connectionId,
                               peerId: socket._peerNodeId || parsedObject?.senderNodeId || peerId,
                               parsedSize: parsedMessageSize,
                               typeHint: parsedObject?.type || 'unknown',
                               keys: Object.keys(parsedObject),
                               sequence: parsedObject?.sequence,
                               totalEntries: parsedObject?.totalEntries
                           });

                           let currentPeerIdForLog = socket._peerNodeId || peerId; // Prefer established socket ID before handlePeerData potentially updates it
                           if (parsedObject.senderNodeId) {
                               currentPeerIdForLog = parsedObject.senderNodeId;
                           }

                           // Determine message type
                           const messageType = parsedObject.type || (parsedObject.requestSync ? 'sync_request' : (parsedObject.sequence ? 'sync_chunk' : (parsedObject.chunk ? 'sync_chunk' : (parsedObject.totalEntries !== undefined ? 'sync_end' : (parsedObject.version !== undefined ? 'metadata_only' : 'unknown')))));
                           this.log('SOCKET_PARSED_MESSAGE_TYPE', { connectionId, peerId: currentPeerIdForLog, messageType });

                           // Route message
                           if (messageType === 'sync_start' || messageType === 'sync_chunk' || messageType === 'sync_end') {
                                this.log('DEBUG_ENTERING_QUEUE_LOGIC', { connectionId, peerId: currentPeerIdForLog, messageType });
                                let queueState = this.syncProcessingState.get(socket);
                                if (!queueState) {
                                     this.log('SYNC_QUEUE_WARNING', { connectionId, peerId: currentPeerIdForLog, error: 'Queue state missing on message arrival, re-initializing.' });
                                     queueState = { queue: [], processing: false };
                                     this.syncProcessingState.set(socket, queueState);
                                }
                                queueState.queue.push(parsedObject);
                                this.log('SYNC_QUEUE_MESSAGE_ADDED', { connectionId, peerId: currentPeerIdForLog, messageType, queueSize: queueState.queue.length });
                                this._processSyncQueue(socket); // Fire and forget (async)
                           } else {
                                this.log('DEBUG_ENTERING_IMMEDIATE_HANDLE_LOGIC', { connectionId, peerId: currentPeerIdForLog, messageType });
                                this.log('SYNC_IMMEDIATE_HANDLE', { connectionId, peerId: currentPeerIdForLog, messageType });
                                // Call handlePeerData IMMEDIATELY for non-chunk/start/end messages
                                await this.handlePeerData(socket, null, parsedObject);
                                this.log('DEBUG_AFTER_AWAIT_HANDLE_PEER_DATA', { connectionId, peerId: socket._peerNodeId || 'unknown_after_handle', messageType }); // Log peerId *after* handlePeerData ran
                           }
                           this.log('DEBUG_AFTER_IF_ELSE', { connectionId, peerId: socket._peerNodeId || 'unknown_after_if', messageType });
                           // *** END MOVED PROCESSING LOGIC ***

                      } catch (parseErr) {
                           // Parse failed - log and keep buffer for next data event
                           this.log('SOCKET_JSON_PARSE_INCOMPLETE', { /* ... */ });
                      }
                  } else {
                       this.log('SOCKET_BUFFER_EMPTY_OR_WHITESPACE', { connectionId, peerId });
                  }
                  this.log('SOCKET_DATA_HANDLER_EXITING', { connectionId, peerId: socket._peerNodeId || 'unknown_at_exit', finalBufferLength: buffer.length });
              })().catch(err => { /* ... */ });
              // *** END More Detailed Logging ***
          }); // End socket.on('data')

          socket.on('close', (hadError) => {
            // *** MODIFIED: Use socket properties directly ***
            const finalPeerId = socket._peerNodeId || 'unknown';
            const finalConnectionId = socket._connectionId || 'unknown';
            this.log('CONNECTION_CLOSED', { connectionId: finalConnectionId, peerId: finalPeerId, hadError });
            // *** END MODIFICATION ***
            this.connections.delete(socket);
            this.syncProcessingState.delete(socket);
            // Reset peer tracking maps for this peer if ID was known
             if (finalPeerId !== 'unknown') {
                 this.lastSyncTimes.delete(finalPeerId);
                 this.peerVersions.delete(finalPeerId);
             }
          });

          socket.on('error', (err) => {
            // *** MODIFIED: Use socket properties directly and fix variable name ***
            const finalPeerId = socket._peerNodeId || 'unknown';
            const finalConnectionId = socket._connectionId || 'unknown';
            this.log('SOCKET_ERROR', { connectionId: finalConnectionId, peerId: finalPeerId, error: err.message, stack: err.stack });
             // *** END MODIFICATION ***
            this.connections.delete(socket);
            this.syncProcessingState.delete(socket);
            this.stats.errors++;
             // Reset peer tracking maps for this peer if ID was known
             if (finalPeerId !== 'unknown') {
                 this.lastSyncTimes.delete(finalPeerId);
                 this.peerVersions.delete(finalPeerId);
             }
          });

        } catch (connSetupErr) { // Catch errors in the main connection setup
           const connId = socket?._connectionId || connectionId || 'setup_failed';
           this.log('CONNECTION_SETUP_ERROR', { connectionId: connId, error: connSetupErr.message, stack: connSetupErr.stack });
           this.stats.errors++;
           if (socket) {
               if (!socket.destroyed) { socket.destroy(connSetupErr); }
               this.connections.delete(socket);
               this.syncProcessingState.delete(socket); // Ensure cleanup here too
           }
        }
      }); // End swarm.on('connection')

      // Handle discovery events
      this.swarm.on('peer', (peer) => {
        try {
          const peerId = typeof peer.publicKey === 'object' ?
            peer.publicKey.toString('hex').slice(0, 8) :
            (peer?.toString ? peer.toString('hex').slice(0, 8) : 'invalid_peer_object'); // Added check
          this.log('DISCOVERED_PEER', { peerIdHint: peerId }); // Changed console.log to this.log
        } catch (err) {
          this.log('DISCOVERED_PEER_ERROR', { error: err.message, peerObject: JSON.stringify(peer) }); // Log error and peer object
        }
      });

      // Handle errors
      this.swarm.on('error', (err) => {
        this.log('SWARM_ERROR', { error: err.message, stack: err.stack }); // Changed console.error to this.log
      });

      // Start listening
      await this.swarm.listen(); // Added await
      console.log('Swarm listening for connections');

      // Join the topic to discover other peers
      const discovery = this.swarm.join(this.topic, { server: true, client: true });
      // Optional: Wait for discovery to settle?
      // await discovery.flushed();
      console.log('Joined swarm with topic');

      // Initial peer discovery
      await this.swarm.flush();
      console.log('Initial peer discovery completed');

      // Add periodic sync every 30 seconds for reliability
      this.syncInterval = setInterval(() => {
        if (this.connections.size > 0) {
          //this.log('Performing periodic sync...');
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
       // Log the startup error
       this.log('STARTUP_ERROR', { error: err.message, stack: err.stack });
       console.error('Error starting P2P network:', err.message); // Keep console for fatal startup errors
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
      // *** Clear the queues map ***
      this.syncProcessingState.clear();
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
    // WARNING: This function loads all non-deleted entries into memory.
    // Prefer using iterators or getAllEntriesStructured for large datasets.
    console.warn('[getAllEntries] Warning: Loading all active entries into memory.'); // Added Warning
    const entries = await this.db._getAllEntries(); // Still loads all initially
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

  // Function to get all entries in the structured format using an iterator
  // Supports pagination via limit and startKey.
  async getAllEntriesStructured(options = {}) {
      const { limit = Infinity, startKey = undefined } = options;
      const data = [];
      const infohashMap = new Map();
      let entriesProcessed = 0;

      // Iterator options: start seeking from startKey if provided
      const iteratorOptions = {};
      if (startKey) {
          iteratorOptions.gt = startKey; // 'gt' ensures we don't include the startKey itself
      }

      const iterator = this.db.db.iterator(iteratorOptions);

      try {
          for await (const [key, entry] of iterator) {
              // Stop if limit is reached
              if (entriesProcessed >= limit) {
                  break;
              }

              // Skip internal metadata keys, test key, and deleted entries
              if (key === '__test__' || key === this.db.METADATA_VERSION_KEY || key === this.db.METADATA_LAST_MODIFIED_KEY || key === this.db.METADATA_ACTIVE_COUNT_KEY || entry.deleted) {
                  continue;
              }

              // Parse the infohash+service key
              const parts = key.split('+');
              if (parts.length < 2) continue; // Skip malformed keys
              const infohash = parts[0];
              const service = parts.slice(1).join('+'); // Handle services with '+' in their name

              // Get or create the infohash entry in the result structure
              let infohashEntry = infohashMap.get(infohash);
              if (!infohashEntry) {
                  infohashEntry = {
                      infohash,
                      services: {}
                  };
                  data.push(infohashEntry);
                  infohashMap.set(infohash, infohashEntry);
              }

              // Convert any string status to boolean if needed
              let cached = entry.cacheStatus;
              if (typeof cached === 'string') {
                  cached = cached === 'completed' || cached === 'cached';
              }

              // Add the service data
              infohashEntry.services[service] = {
                  cached: cached,
                  last_modified: entry.timestamp,
                  expiry: entry.expiration
              };

              entriesProcessed++; // Count entries added to the result structure
          }
      } catch (err) {
          console.error('Error iterating through entries for structured format:', err);
          this.log('STRUCTURED_FORMAT_ERROR', { error: err.message, stack: err.stack });
          // Depending on requirements, might want to return partial data or throw
      } finally {
          await iterator.close().catch(err => console.error('Error closing structured format iterator:', err));
      }

      // Clear the map to free memory before returning
      infohashMap.clear();

      // Return the data page
      // Note: The *caller* (e.g., REST API) needs to handle getting the 'nextKey'
      // for subsequent pages if needed, potentially by taking the last key from this page.
      return { data };
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
  async syncWithPeers(trigger = 'local_change', parentSyncChain = []) { // Added parentSyncChain
    this.log('SYNC_TRIGGERED', { trigger, peerCount: this.connections.size, parentChainLength: parentSyncChain.length }); // Log chain info
    for (const socket of this.connections) {
        if (!socket.destroyed) {
            const peerId = socket._peerNodeId || 'unknown';
            try {
                // *** CHANGE 2: Check if peer is anywhere in the chain ***
                let chainToSendToPeer;
                const peerIsInChain = peerId !== 'unknown' && parentSyncChain.includes(peerId);

                if (peerIsInChain) {
                    // Don't propagate the existing chain back to a node already in the path.
                    // Send metadata reflecting the current node's state, starting a fresh chain from here.
                    this.log('SYNC_WITH_PEERS_SKIP_CHAIN_PROPAGATION_TO_ANCESTOR', { trigger, peerId, originalChain: parentSyncChain });
                    chainToSendToPeer = []; // Start fresh chain
                } else {
                    // This peer is not in the parent chain, propagate the chain normally.
                    chainToSendToPeer = parentSyncChain;
                }
                // *** Pass the potentially modified chain ***
                await this.handleSyncRequest(socket, trigger, chainToSendToPeer); // Use potentially modified chain
            } catch (err) {
                this.log('SYNC_PEER_ERROR', { peerId, trigger, error: err.message });
                this.stats.errors++;
            }
        } else {
            this.connections.delete(socket);
        }
    }
}

  // *** NEW: Centralized Cleanup Function ***
  _cleanupSyncState(socket) {
      if (!socket) return;
      const peerId = socket._peerNodeId || 'unknown';
      // Check if already cleaned
      if (socket._syncBatch === undefined && socket._activeChunkHandlers === undefined && socket._currentSyncChain === undefined) { // Also check _currentSyncChain
          return;
      }
      // ... (close batch logic) ...

      // Nullify all sync-related properties
      socket._syncBatch = undefined;
      socket._syncProcessedCounter = undefined;
      socket._syncTotalWrittenCounter = undefined;
      socket._syncInitialLocalVersion = undefined;
      socket._syncInitialLocalLastModified = undefined;
      socket._syncRemoteMeta = undefined;
      socket._activeChunkHandlers = undefined;
      socket._syncReadyToClean = undefined;
      socket._syncNetCountChange = undefined; // Also clear this
      socket._syncInitialLocalCount = undefined; // And this
      socket._currentSyncChain = undefined; // *** Clear the stored sync chain ***

      this.log('SYNC_CHUNK_STATE_CLEANED (centralized)', { connectionId: socket._connectionId || 'N/A', peerId });
  }

  // *** MODIFIED: sendFullStateChunks to load data first ***
  async sendFullStateChunks(socket, trigger, syncChain) {
      const peerId = socket._peerNodeId || 'unknown';
      const logContext = { peerId, trigger, connectionId: socket._connectionId || 'N/A' }; // Base context for logging
      this.log('SENDING_FULL_STATE_CHUNKS (Load First)', { ...logContext }); // Indicate new strategy

      let allEntries = []; // Array to hold all DB entries
      try {
          // --- Step 1: Load all relevant data from DB ---
          this.log('SEND_CHUNKS_LOADING_DB', { ...logContext });
          const loadStartTime = Date.now();
          const iterator = this.db.db.iterator();
          try {
              for await (const [key, value] of iterator) {
                  // Skip internal keys and test key
                  if (key === this.db.METADATA_VERSION_KEY || key === this.db.METADATA_LAST_MODIFIED_KEY || key === this.db.METADATA_ACTIVE_COUNT_KEY || key === '__test__') continue;
                  allEntries.push({ key, value });
              }
          } finally {
              await iterator.close().catch(err => console.error('Error closing load iterator:', err));
          }
          const loadDuration = Date.now() - loadStartTime;
          this.log('SEND_CHUNKS_DB_LOADED', { ...logContext, entryCount: allEntries.length, durationMs: loadDuration });
          // --- End Step 1 ---


          // --- Step 2: Send data in chunks with backpressure ---
          const chunkSize = 100;
          let sequence = 0;
          const totalEntries = allEntries.length;

          // Send sync_start marker
          const startPayload = {
              type: 'sync_start',
              senderNodeId: this.nodeId,
              syncChain: syncChain,
              version: this.db.version,
              lastModified: this.db.lastModified
          };
          const startMessage = JSON.stringify(startPayload);
          await this._writeDataWithBackpressure(socket, startMessage, { ...logContext, type: 'sync_start' });
          this.log('SYNC_CHUNK_SENT_START', { ...logContext, version: this.db.version });

          // Loop through the in-memory array
          for (let i = 0; i < totalEntries; i += chunkSize) {
              sequence++;
              const chunk = allEntries.slice(i, i + chunkSize); // Get chunk from array
              const chunkPayload = { type: 'sync_chunk', senderNodeId: this.nodeId, syncChain, sequence, chunk };
              const chunkMessage = JSON.stringify(chunkPayload);
              await this._writeDataWithBackpressure(socket, chunkMessage, { ...logContext, type: 'sync_chunk', sequence });
              // Optional: Log chunk sent
              if (sequence % 10 === 1) { // Log progress occasionally
                 this.log('SEND_CHUNK_PROGRESS (Load First)', { ...logContext, sequence, sent: i + chunk.length, total: totalEntries });
              }
          }

          // Clear the large array now that we're done sending
          this.log('SEND_CHUNK_CLEARING_ARRAY', { ...logContext, sizeBeforeClear: allEntries.length });
          allEntries = []; // Help GC

          // Send sync_end marker
          const endPayload = {
              type: 'sync_end',
              senderNodeId: this.nodeId,
              syncChain: syncChain,
              totalEntries: totalEntries, // Send total count
              version: this.db.version, // Send current version
              lastModified: this.db.lastModified // Send current mod time
          };
          const endMessage = JSON.stringify(endPayload);
          await this._writeDataWithBackpressure(socket, endMessage, { ...logContext, type: 'sync_end' });
          this.log('SYNC_CHUNK_SENT_END', { ...logContext, totalEntries, sequence });
          // --- End Step 2 ---

      } catch (err) {
          // Clear array on error too
          this.log('SEND_CHUNK_CLEARING_ARRAY_ON_ERROR', { ...logContext, sizeBeforeClear: allEntries?.length || 0 });
          allEntries = []; // Help GC
          this.log('SEND_CHUNKS_ERROR (Load First)', { ...logContext, error: err.message, stack: err.stack });
          this.stats.errors++;
          try {
              const errorPayload = { type: 'sync_error', senderNodeId: this.nodeId, error: 'Failed to send full state' };
              socket.write(JSON.stringify(errorPayload));
          } catch (writeErr) { /* Ignore */ }
      }
  }

  // *** REPLACED: Corrected Queue processor function ***
  async _processSyncQueue(socket) {
    const peerId = socket._peerNodeId || 'unknown';
    const logContext = { connectionId: socket._connectionId || 'N/A', peerId };

    let queueState = this.syncProcessingState.get(socket);
    if (!queueState) {
      this.log('SYNC_QUEUE_ERROR', { ...logContext, error: 'Queue state not found for socket during processing' });
      return;
    }

    // If already processing or queue is empty, do nothing
    if (queueState.processing || queueState.queue.length === 0) {
      return;
    }

    queueState.processing = true;
    const parsedMessage = queueState.queue.shift(); // Get the next message

    // Determine message type (simplified logic for queued messages)
    let messageType = parsedMessage.type; // Prefer explicit type
    if (!messageType) {
      // Infer based on expected queue contents (start, chunk, end)
      if (parsedMessage.sequence || parsedMessage.chunk) {
          messageType = 'sync_chunk';
      } else if (parsedMessage.totalEntries !== undefined) {
          messageType = 'sync_end';
      } else if (parsedMessage.version !== undefined) {
          // If it has version but no sequence/chunk/totalEntries, it must be sync_start
          // as only sync_start, sync_chunk, sync_end should be in this queue.
          messageType = 'sync_start';
      } else {
          messageType = 'unknown'; // Fallback, should not happen ideally
      }
      // Log if type had to be inferred (might indicate malformed message)
      if(messageType !== 'unknown') {
           this.log('SYNC_QUEUE_WARNING', { ...logContext, warning: `Inferred message type as '${messageType}' because 'type' field was missing`, parsedKeys: Object.keys(parsedMessage) });
      }
    }

    // Log processing start (optional)
    // this.log('SYNC_QUEUE_PROCESSING_MESSAGE', { ...logContext, messageType, queueSize: queueState.queue.length });

    try {
      // Call handlePeerData to process the dequeued message
      await this.handlePeerData(socket, null, parsedMessage);
    } catch (err) {
      // Log errors during processing, but continue processing the queue
      this.log('SYNC_QUEUE_HANDLER_ERROR', { ...logContext, messageType, error: err.message, stack: err.stack });
      // Consider more robust error handling, e.g., clearing the queue or closing socket on specific errors
    } finally {
      queueState.processing = false;
      // Use setImmediate to yield control briefly and prevent potential stack overflow
      setImmediate(() => this._processSyncQueue(socket));
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

  // *** NEW: Log PID for easy signal sending ***
  console.log(`P2PDBClient process running with PID: ${process.pid}`);
  // *** END NEW ***
}
