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
      const receivedDataLength = data.length; // Store original data length

      try {
          // --- TEMPORARILY REDUCED LOGGING ---
          // this.log('PEER_DATA_RAW_RECEIVED', { connectionId: socket._connectionId || 'N/A', peerId: currentPeerId, dataSize: receivedDataLength });

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
                  this.log('PEER_ID_UPDATED', { connectionId: socket._connectionId || 'N/A', oldId: currentPeerId, newId: messageSenderId }); // Added connectionId

                  // Optional: Clean up old 'unknown' or incorrect entries
                  if (currentPeerId !== 'unknown') {
                     this.lastSyncTimes.delete(currentPeerId);
                     this.peerVersions.delete(currentPeerId);
                  }
              }
          } else if (peerId === 'unknown') {
              // Still unknown and message has no ID - problematic
              this.log('PEER_DATA_WARNING', { connectionId: socket._connectionId || 'N/A', message: "Received data without senderNodeId from unidentified peer. Ignoring.", rawDataSample: data.toString('utf8').substring(0, 200) }); // Added connectionId
              return; // Ignore data if we can't identify the sender
          }
          // From here, 'peerId' reliably holds the sender's ID

          socket._syncChain = parsed.syncChain || [];

          this.stats.syncsReceived++;

          // Infer message type more robustly
          // REMOVED: 'full_state' inference based on parsed.entries
          const messageType = parsed.type || (parsed.requestSync ? 'sync_request' : (parsed.sequence ? 'sync_chunk' : (parsed.chunk ? 'sync_chunk' : (parsed.totalEntries !== undefined ? 'sync_end' : (parsed.version !== undefined ? 'metadata_only' : 'unknown')))));


          // --- TEMPORARILY REDUCED LOGGING ---
          /*
          this.log('PEER_DATA', {
              connectionId: socket._connectionId || 'N/A', // Added connectionId
              peerId,
              messageType,
              syncChain: socket._syncChain,
              dataSize: receivedDataLength // Use stored length
          });
          */

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

          // --- Handle Metadata & Chunks ---

          // Update peerVersions map using the authoritative peerId
          if (parsed.version !== undefined) {
              this.peerVersions.set(peerId, parsed.version);
              // --- TEMPORARILY REDUCED LOGGING ---
              // this.log('PEER_VERSION_UPDATE', { connectionId: socket._connectionId || 'N/A', peerId: peerId, version: parsed.version }); // Added connectionId
          }

          // --- Check if we need to request a full sync based on metadata ---
          const remoteVersion = parsed.version;
          const remoteLastModified = parsed.lastModified;
          const localVersion = this.db.version;
          const localLastModified = this.db.lastModified;

          // Check only if we received metadata
          // SIMPLIFIED: Removed check for empty 'full_state' as legacy full_state is gone.
          if (messageType === 'metadata_only' &&
              // *** Use Date.parse() for comparison ***
              (remoteVersion > localVersion || (remoteLastModified && localLastModified && Date.parse(remoteLastModified) > Date.parse(localLastModified)))) {

              // Check cooldown before requesting again from this specific peer
              const now = Date.now();
              const lastSyncReqTime = socket._lastSyncRequestTime || 0;
              const syncRequestCooldown = 10000; // 10 seconds cooldown for requesting

              if (now - lastSyncReqTime > syncRequestCooldown) {
                  this.log('REQUESTING_SYNC_FROM_PEER', {
                      connectionId: socket._connectionId || 'N/A', // Added connectionId
                      peerId,
                      reason: 'remote_newer_metadata',
                      remoteVersion, remoteLastModified,
                      localVersion, localLastModified
                  });

                  const requestPayload = {
                      type: 'sync_request', // Use type field
                      requestSync: true, // Keep for potential backward compatibility
                      requestType: 'full_state', // *** Explicitly request full state (which triggers chunking) ***
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
                      connectionId: socket._connectionId || 'N/A', // Added connectionId
                      peerId,
                      reason: 'request_cooldown',
                      timeSinceLastReq: now - lastSyncReqTime
                  });
              }
          }
          // --- End Check ---

          // --- Handle Incoming Chunks with Direct Batching ---
          const BATCH_WRITE_THRESHOLD = 1000; // Write batch every 1000 entries processed

          if (messageType === 'sync_start') {
              // Initialize state for chunked sync on the socket
              if (socket._syncBatch) {
                 this.log('SYNC_CHUNK_WARNING_BATCHING', { connectionId: socket._connectionId || 'N/A', peerId, error: 'Received sync_start while already in a sync batch state. Ignoring new start.'});
                 return;
              }
              socket._syncBatch = this.db.db.batch(); // LevelDB batch
              socket._syncProcessedCounter = 0;       // Count entries processed in current batch
              socket._syncTotalWrittenCounter = 0;   // Count total entries written in this sync
              socket._syncInitialLocalVersion = this.db.version; // Store local state before merge
              socket._syncInitialLocalLastModified = this.db.lastModified;
              // << REMOVED: No longer storing initial count or net change >>
              socket._syncRemoteMeta = { // Store metadata associated with this sync
                  version: parsed.version,
                  lastModified: parsed.lastModified
              };
              socket._activeChunkHandlers = 0; // Initialize counter
              socket._syncReadyToClean = false; // Initialize flag

              this.log('SYNC_CHUNK_START_BATCHING', {
                  connectionId: socket._connectionId || 'N/A', // Added connectionId
                  peerId,
                  remoteVersion: parsed.version,
                  remoteLastModified: parsed.lastModified,
                  initialLocalVersion: socket._syncInitialLocalVersion,
                  initialLocalLastModified: socket._syncInitialLocalLastModified,
                  // Log current count at start for reference, but it's not stored on socket anymore
                  initialLocalCount: this.db.activeEntriesCount
              });
              return; // Wait for chunks
          }

          if (messageType === 'sync_chunk') {
              let currentBatchForChunk = socket._syncBatch; // Get reference to the batch active *at the start* of processing this chunk

              // Check if a batch exists when the chunk arrives
              if (!currentBatchForChunk) {
                  // If no batch, but we have active handlers, it means sync_end cleaned up early.
                  // If no batch AND no handlers, it's likely an orphan chunk after error/close.
                  const reason = socket._activeChunkHandlers > 0 ? 'sync_end_cleaned_early' : 'no_sync_batch_active';
                  this.log('SYNC_CHUNK_ERROR_BATCHING', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, error: `Received chunk but ${reason}` });
                  return; // Ignore orphan or late chunk
              }

              const chunkEntries = parsed.chunk || [];
              // Log chunk reception summary, less frequently if needed
              if (parsed.sequence % 10 === 1) { // Log every 10th chunk summary
                this.log('SYNC_CHUNK_RECEIVED_BATCHING', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, chunkSize: chunkEntries.length }); // Added connectionId
              }

              socket._activeChunkHandlers++; // Increment before processing entries
              let processingErrorOccurred = false; // Flag to stop processing on error
              try {
                  for (const entry of chunkEntries) {
                      if (processingErrorOccurred) break; // Stop if previous iteration had error

                      // Ensure entry is valid
                      if (!entry || entry.key === undefined || entry.value === undefined ||
                          entry.key === this.db.METADATA_VERSION_KEY || entry.key === this.db.METADATA_LAST_MODIFIED_KEY || entry.key === '__test__') {
                          // --- TEMPORARILY REDUCED LOGGING ---
                          // this.log('SYNC_CHUNK_WARNING_BATCHING', { connectionId: socket._connectionId || 'N/A', peerId, message: 'Skipping invalid or internal entry in chunk', entryKey: entry?.key }); // Added connectionId
                          continue;
                      }

                      // --- Process Entry ---
                      try {
                          // *** REDUCED LOGGING inside loop ***
                          // this.log('SYNC_CHUNK_PROCESS_ENTRY_START', { connectionId: socket._connectionId || 'N/A', peerId, key: entry.key });

                          const localEntry = await this.db.db.get(entry.key).catch(() => null);

                          // this.log('SYNC_CHUNK_PROCESS_ENTRY_GOT_LOCAL', { connectionId: socket._connectionId || 'N/A', peerId, key: entry.key, found: !!localEntry });


                          // --- PRIMARY CONCURRENCY CHECK (Still useful for intermediate writes) ---
                          if (!socket._syncBatch) {
                              // This specific warning might still occur if an error happens elsewhere during this chunk's processing
                              this.log('SYNC_CHUNK_WARNING_BATCHING (Inside Loop)', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, message: 'Sync batch became null during entry processing.', entryKey: entry.key });
                              processingErrorOccurred = true;
                              break; // Stop processing this chunk
                          }
                          if (socket._syncBatch !== currentBatchForChunk) {
                              this.log('SYNC_CHUNK_WARNING_BATCHING (Inside Loop)', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, message: 'Batch replaced concurrently during entry processing.', entryKey: entry.key });
                              processingErrorOccurred = true;
                              break; // Stop processing this chunk
                          }
                          // --- END PRIMARY CHECKS ---

                          // Merge logic: remote is newer if no local entry OR remote timestamp is later
                          // *** Use Date.parse() for comparison ***
                          const shouldUpdate = !localEntry || (localEntry.timestamp && entry.value.timestamp && Date.parse(localEntry.timestamp) < Date.parse(entry.value.timestamp));

                          if (shouldUpdate) {
                              // << REMOVED: Count change calculation and accumulation >>

                              try {
                                  currentBatchForChunk.put(entry.key, entry.value);
                                  socket._syncProcessedCounter++;
                                  socket._syncTotalWrittenCounter++;

                                  // << REMOVED: Accumulate count change logs >>

                              } catch (putErr) {
                                  // Specifically check if the error is due to the batch being closed
                                  if (putErr.message.includes('Batch is not open')) {
                                      this.log('SYNC_CHUNK_RACE_CONDITION_CAUGHT (Put)', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, error: `Batch closed concurrently before put for key ${entry.key}. Stopping chunk.`, message: putErr.message }); // Added connectionId, sequence
                                  } else {
                                      // Log other unexpected errors during put
                                      this.log('SYNC_CHUNK_PUT_ERROR', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, error: `Error during put for key ${entry.key}`, message: putErr.message }); // Added connectionId, sequence
                                  }
                                  processingErrorOccurred = true; // Stop processing the rest of the chunk
                                  break; // Exit the loop
                              }

                              // --- Check for Intermediate Write ---
                              if (socket._syncProcessedCounter >= BATCH_WRITE_THRESHOLD) {
                                  // ... log threshold reached ...
                                  try {
                                     // ... write intermediate batch ...
                                     // ... create new batch ...
                                  } catch (writeErr) {
                                      // ... handle intermediate write error ...
                                      processingErrorOccurred = true;
                                      break;
                                  }
                              } // End threshold check
                          } // End shouldUpdate
                      } catch (outerErr) {
                          // Catch errors during the get() or the outer checks
                          this.log('SYNC_CHUNK_ENTRY_PROCESSING_ERROR (Outer)', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence, error: `Error processing chunk entry for key ${entry.key}`, message: outerErr.message }); // Added connectionId, sequence
                          processingErrorOccurred = true; // Signal to stop loop on any error
                          break; // Stop processing the rest of the chunk on error
                      }
                      // --- End Process Entry ---

                  } // End of for loop over chunk entries
              } finally {
                  socket._activeChunkHandlers--; // Decrement after processing entries or on error
                  if (processingErrorOccurred) {
                      this.log('SYNC_CHUNK_PROCESSING_ERROR_FLAG_SET', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence }); // Added connectionId, sequence
                  }
                  // Check if this was the last handler AND sync_end already ran
                  if (socket._activeChunkHandlers === 0 && socket._syncReadyToClean) {
                      this.log('SYNC_CHUNK_PERFORMING_DEFERRED_CLEANUP', { connectionId: socket._connectionId || 'N/A', peerId, sequence: parsed.sequence });
                      this._cleanupSyncState(socket);
                  }
              }

          } // End messageType === 'sync_chunk'


          if (messageType === 'sync_end') {
              let finalBatch = socket._syncBatch; // Get ref to the batch active when sync_end arrives

              if (!finalBatch) {
                  // Sync might have already errored or been cleaned up
                  const reason = socket._activeChunkHandlers > 0 ? 'batch_already_null_but_handlers_active' : 'no_batch_active';
                   this.log('SYNC_CHUNK_ERROR_BATCHING (sync_end)', { connectionId: socket._connectionId || 'N/A', peerId, reason: reason });
                   // Reset flags just in case
                   socket._syncReadyToClean = false;
                   // Don't reset activeChunkHandlers here, let them decrement naturally
                   return;
              }

              // << REMOVED: finalNetCountChange calculation >>

              this.log('SYNC_CHUNK_END_BATCHING', { connectionId: socket._connectionId || 'N/A', peerId, finalBatchSize: finalBatch.length, totalEntriesExpected: parsed.totalEntries, totalActuallyProcessed: socket._syncTotalWrittenCounter }); // Logging remains similar, just without netChange

              let changesMade = false; // Track if any DB write occurs
              let recalculatedCount = -1; // Store recount result, initialized to invalid value

              try {
                  // Write the final batch if it contains operations
                  if (finalBatch && finalBatch.length > 0) { // Added check for finalBatch existence
                      this.log('SYNC_CHUNK_WRITING_FINAL_BATCH', { connectionId: socket._connectionId || 'N/A', peerId, batchSize: finalBatch.length }); // Added connectionId
                      const writeStartTime = Date.now();
                      await finalBatch.write(); // Write the specific final batch instance
                      const writeDuration = Date.now() - writeStartTime;
                      this.log('SYNC_CHUNK_FINAL_BATCH_WRITE_SUCCESS', { connectionId: socket._connectionId || 'N/A', peerId, batchSize: finalBatch.length, durationMs: writeDuration }); // Added connectionId, duration
                      changesMade = true; // Mark changes made if final batch was written
                  } else {
                       this.log('SYNC_CHUNK_FINAL_BATCH_EMPTY', { connectionId: socket._connectionId || 'N/A', peerId }); // Added connectionId
                  }

                  // Verify total entry count (use the counter of entries added to batches)
                  if (socket._syncTotalWrittenCounter !== parsed.totalEntries) {
                      this.log('SYNC_CHUNK_ERROR_BATCHING', {
                       connectionId: socket._connectionId || 'N/A', // Added connectionId
                       peerId,
                          error: 'sync_end totalEntries mismatch',
                       expected: parsed.totalEntries,
                          written: socket._syncTotalWrittenCounter // Log the count we tracked
                      });
                      // Potentially revert changes or log inconsistency? For now, just log.
                  } else {
                      this.log('SYNC_CHUNK_ENTRY_COUNT_MATCH', { connectionId: socket._connectionId || 'N/A', peerId, count: socket._syncTotalWrittenCounter }); // Added connectionId
                  }

                  // --- Update Local Metadata ---
                  const remoteVersion = parsed.version !== undefined ? parsed.version : socket._syncRemoteMeta?.version;
                  const remoteLastModified = parsed.lastModified !== undefined ? parsed.lastModified : socket._syncRemoteMeta?.lastModified;
                  const initialLocalVersion = socket._syncInitialLocalVersion;
                  const initialLocalLastModified = socket._syncInitialLocalLastModified;
                  // << REMOVED: initialLocalCount usage here >>
                  const remoteStateWasNewer = (remoteVersion > initialLocalVersion || (remoteLastModified && initialLocalLastModified && Date.parse(remoteLastModified) > Date.parse(initialLocalLastModified)));

                  // Update metadata if changes were written OR remote was newer
                  if (changesMade || remoteStateWasNewer) {
                     const metadataBatch = this.db.db.batch();
                     const entriesWereWritten = socket._syncTotalWrittenCounter > 0; // Check if we actually wrote entries
                     const shouldIncrementVersion = entriesWereWritten || (remoteStateWasNewer && !entriesWereWritten);
                     const newVersion = Math.max(initialLocalVersion, remoteVersion || 0) + (shouldIncrementVersion ? 1 : 0);
                     const newLastModified = Date.now();

                     // << RECALCULATE COUNT >>
                     this.log('SYNC_CHUNK_RECALCULATING_COUNT', { connectionId: socket._connectionId || 'N/A', peerId, reason: `changesMade: ${changesMade}, remoteNewer: ${remoteStateWasNewer}` });
                     const recountStartTime = Date.now();
                     recalculatedCount = await this.db._calculateActiveEntries(); // Perform full recount
                     const recountDuration = Date.now() - recountStartTime;
                     this.log('SYNC_CHUNK_RECALCULATION_COMPLETE', { connectionId: socket._connectionId || 'N/A', peerId, newCount: recalculatedCount, durationMs: recountDuration });
                     // << END RECALCULATION >>

                     metadataBatch.put(this.db.METADATA_VERSION_KEY, newVersion);
                     metadataBatch.put(this.db.METADATA_LAST_MODIFIED_KEY, newLastModified);
                     metadataBatch.put(this.db.METADATA_ACTIVE_COUNT_KEY, recalculatedCount); // Write the recalculated count

                     this.log('SYNC_CHUNK_WRITING_METADATA', { connectionId: socket._connectionId || 'N/A', peerId, newVersion, oldVersion: this.db.version, newCount: recalculatedCount }); // Log new count

                     const metaWriteStartTime = Date.now();
                     await metadataBatch.write();
                     const metaWriteDuration = Date.now() - metaWriteStartTime;

                     // Update in-memory state only after successful write
                     this.db.version = newVersion;
                     this.db.lastModified = newLastModified;
                     this.db.activeEntriesCount = recalculatedCount; // Update in-memory count

                     this.log('SYNC_CHUNK_METADATA_UPDATED', { connectionId: socket._connectionId || 'N/A', peerId, newVersion, newLastModified: new Date(newLastModified).toISOString(), newActiveCount: this.db.activeEntriesCount, durationMs: metaWriteDuration });
                     // Ensure changesMade is true if we updated metadata
                     changesMade = true;
                  } else {
                       this.log('SYNC_CHUNK_METADATA_NO_UPDATE', { connectionId: socket._connectionId || 'N/A', peerId, reason: 'Remote state not newer and no entries written' });
                  }

                  // --- Notify Peers ---
                  if (changesMade) {
                      // Log the count (which might be the recalculated one or the existing one if no metadata was updated)
                      console.log(`Batch sync completed from ${peerId} (Conn: ${socket._connectionId || 'N/A'}). ${socket._syncTotalWrittenCounter || 0} entries processed. New version: ${this.db.version}. New Count: ${this.db.activeEntriesCount}.`);
                      this.stats.changesMerged += socket._syncTotalWrittenCounter || 0;
                      this.stats.lastChangeAt = new Date().toISOString();
                      this.syncWithPeers(`sync_complete_from_${peerId}`);
                  } else {
                       console.log(`Batch sync completed from ${peerId} (Conn: ${socket._connectionId || 'N/A'}). No changes applied. Version: ${this.db.version}. Count: ${this.db.activeEntriesCount}.`);
                   }

              } catch (err) {
                   this.log('SYNC_CHUNK_FINAL_PROCESSING_ERROR', { connectionId: socket._connectionId || 'N/A', peerId, error: err.message, stack: err.stack }); // Added connectionId
                   // Handle error during final write or metadata update
                   // Ensure batch is closed if write failed
                   if (finalBatch && !finalBatch.closed) {
                       try { await finalBatch.close(); } catch(closeErr) {/* ignore */}
                   }
              } finally {
                  // --- Cleanup Decision Point ---
                  if (socket._activeChunkHandlers === 0) {
                      // No chunk handlers are running, safe to clean up immediately
                      this.log('SYNC_CHUNK_CLEANING_IMMEDIATELY (sync_end)', { connectionId: socket._connectionId || 'N/A', peerId });
                      this._cleanupSyncState(socket);
                  } else {
                      // Chunk handlers still running, defer cleanup
                      socket._syncReadyToClean = true;
                      this.log('SYNC_CHUNK_DEFERRING_CLEANUP (sync_end)', { connectionId: socket._connectionId || 'N/A', peerId, activeHandlers: socket._activeChunkHandlers });
                  }
                  // --- End Cleanup Decision Point ---
              }
              return; // Sync complete
          }
          // --- End Chunk Handling ---


          // --- REMOVED: Merge Logic for LEGACY non-chunked full_state messages ---
          // The entire block starting with:
          // if (messageType === 'full_state' && parsed.entries) { ... }
          // has been removed.


      } catch (err) {
         // Make sure peerId is defined for logging, fallback if necessary
         const errorPeerId = socket?._peerNodeId || currentPeerId || 'unknown';
         // Log the original data length on parse error
         if (err instanceof SyntaxError) {
             this.log('PEER_DATA_PARSE_ERROR', {
                 connectionId: socket._connectionId || 'N/A', // Added connectionId
                 peerId: errorPeerId,
                 error: err.message,
                 dataSize: receivedDataLength, // Log size causing parse error
                 rawDataSample: data.toString('utf8').substring(0, 200)
             });
         } else {
             this.log('PEER_DATA_ERROR', {
                 connectionId: socket._connectionId || 'N/A', // Added connectionId
                 peerId: errorPeerId,
                 error: err.message,
                 stack: err.stack,
                 dataSize: receivedDataLength, // Log size on other errors too
                 rawDataSample: data.toString('utf8').substring(0, 200)
             });
         }
         this.stats.errors++;
         // Clean up potentially corrupted sync state on the socket on any error
         if (socket) {
             if (socket._syncBatch && !socket._syncBatch.closed) { // Attempt to close lingering batch on error
                 try { await socket._syncBatch.close(); } catch(closeErr) { /* ignore */ }
             }
             socket._syncBatch = null;
             socket._syncProcessedCounter = null;
             socket._syncTotalWrittenCounter = null;
             socket._syncInitialLocalVersion = null;
             socket._syncInitialLocalLastModified = null;
             socket._syncRemoteMeta = null;
         }
         // Remove logger if it was attached to db instance
         // if (this.db && this.db.log) delete this.db.log; // Let's keep the logger attached
      }
  }

  // *** NEW: Helper function to send full state in chunks ***
  async sendFullStateChunks(socket, trigger, syncChain) {
      const peerId = socket._peerNodeId || 'unknown';
      this.log('SENDING_FULL_STATE_CHUNKS', { peerId, trigger });

      try {
          const chunkSize = 250; // << REDUCED CHUNK SIZE
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
        let connectionId = null; // Assign later
        try {
          // We'll get the actual nodeId from the sync messages
          this.connections.add(socket);
          this.stats.connectionsTotal++;
          connectionId = `conn_${Date.now().toString(36)}_${Math.random().toString(36).substring(2, 7)}`; // Unique ID for logging this connection instance
          socket._connectionId = connectionId; // Attach for logging

          this.log('NEW_CONNECTION', {
            connectionId,
            peerId: socket._peerNodeId || 'unknown', // Still unknown initially
            remoteAddress: info?.remoteAddress, // Log remote address if available
            type: info?.type // client or server
          });

          // Initial sync - share our database state
          this.handleSyncRequest(socket, 'initial_connection').catch(err => {
            this.log('SYNC_ERROR_INITIAL', { connectionId, error: err.message, stack: err.stack });
            this.stats.errors++;
          });

          // Handle incoming data from peers
          let buffer = '';
          let peerNodeId = null; // Track nodeId identified for this socket
          const MAX_BUFFER_SIZE = 50 * 1024 * 1024; // 50MB limit per connection buffer

          // *** REFACTORED: Incremental JSON parsing logic ***
          socket.on('data', data => {
            const receivedDataLength = data.length;
            buffer += data.toString('utf8'); // Append new data

            // Check if buffer exceeds limit *after* appending
            if (buffer.length > MAX_BUFFER_SIZE) {
                this.log('SOCKET_BUFFER_LIMIT_EXCEEDED', { connectionId, peerId: socket._peerNodeId || 'unknown', bufferSize: buffer.length, limitMB: MAX_BUFFER_SIZE / (1024 * 1024) });
                buffer = ''; // Clear the buffer
                this.stats.errors++;
                socket.destroy(new Error(`Incoming buffer limit exceeded (${MAX_BUFFER_SIZE / (1024 * 1024)}MB)`));
                return; // Stop processing this connection
            }

            // Process buffer asynchronously to avoid blocking the event loop for too long
            (async () => {
              let processed = true; // Flag to indicate if we made progress parsing
              while (processed && buffer.length > 0) {
                  processed = false; // Assume no progress until a message is parsed
                  try {
                      // Find the first potential start of a JSON object/array
                      const firstBrace = buffer.indexOf('{');
                      const firstBracket = buffer.indexOf('[');
                      let potentialStart = -1;

                      if (firstBrace !== -1 && (firstBracket === -1 || firstBrace < firstBracket)) {
                          potentialStart = firstBrace;
                      } else if (firstBracket !== -1) {
                          potentialStart = firstBracket;
                      }

                      // If no JSON start found, or only whitespace before it, clear buffer (or the whitespace part)
                      if (potentialStart === -1) {
                          if (buffer.trim().length === 0) {
                              // Buffer contains only whitespace, clear it
                              buffer = '';
                          }
                          // If non-whitespace junk before potential JSON, keep it for now? Or log/discard?
                          // For now, we break the loop, assuming we need more data to form a valid start.
                          break;
                      }

                      // Discard data before the first potential JSON start
                      if (potentialStart > 0) {
                          const discardedData = buffer.substring(0, potentialStart);
                          this.log('SOCKET_BUFFER_DISCARDING_PREFIX', { connectionId, peerId: socket._peerNodeId || 'unknown', discardedLength: discardedData.length, prefixSample: discardedData.substring(0, 100) });
                          buffer = buffer.substring(potentialStart);
                      }

                      // Attempt to parse the remaining buffer
                      const parsed = JSON.parse(buffer);
                      const parsedMessageSize = Buffer.byteLength(buffer, 'utf8'); // Size of the successfully parsed message

                      // If successful, log and reset the buffer entirely (as we parsed the whole remaining content)
                      this.log('SOCKET_BUFFER_PARSE_SUCCESS', { connectionId, peerId: socket._peerNodeId || parsed?.senderNodeId || 'unknown', parsedSize: parsedMessageSize });
                      const dataToProcess = buffer; // Capture the buffer content *before* clearing
                      buffer = ''; // Consume the entire buffer

                      // Update peer's nodeId if we receive it
                      if (parsed.senderNodeId && !peerNodeId) {
                          peerNodeId = parsed.senderNodeId;
                          socket._peerNodeId = peerNodeId; // Update socket property
                          this.log('NEW_PEER_IDENTIFIED', { connectionId, peerId: peerNodeId });
                      } else if (parsed.senderNodeId && peerNodeId && parsed.senderNodeId !== peerNodeId) {
                          this.log('PEER_ID_CHANGED', { connectionId, oldId: peerNodeId, newId: parsed.senderNodeId });
                          peerNodeId = parsed.senderNodeId; // Update tracked ID
                          socket._peerNodeId = peerNodeId; // Update socket property
                      }

                      // Pass the *original string data* that was successfully parsed
                      await this.handlePeerData(socket, null, dataToProcess); // Pass the original string
                      processed = true; // We successfully processed a message

                  } catch (parseErr) {
                      // If we can't parse the JSON yet, it's likely an incomplete message.
                      if (parseErr instanceof SyntaxError) {
                          // Expected error for incomplete JSON. Break the inner loop and wait for more data.
                          // Optional: Add logging if buffer gets large and still incomplete
                          // if (buffer.length > 1 * 1024 * 1024) { this.log(...) }
                          break; // Exit the while loop, wait for more data
                      } else {
                          // Unexpected error during parse (not SyntaxError) - Log and clear buffer to prevent infinite loops
                          this.log('SOCKET_BUFFER_PARSE_UNEXPECTED_ERROR', { connectionId, peerId: socket._peerNodeId || 'unknown', bufferSize: buffer.length, error: parseErr.message, stack: parseErr.stack, bufferSample: buffer.substring(0, 200) });
                          buffer = ''; // Clear buffer on unexpected errors
                          this.stats.errors++;
                          break; // Exit the while loop
                      }
                  }
              } // End while(processed && buffer.length > 0)
            })().catch(err => {
                // Catch errors from the async IIFE itself or handlePeerData
                this.log('SOCKET_DATA_ASYNC_HANDLER_ERROR', { connectionId, peerId: socket._peerNodeId || 'unknown', error: err.message, stack: err.stack });
                buffer = ''; // Reset buffer on error
                this.stats.errors++;
            });
          });
          // *** END REFACTORED Logic ***


          // Handle disconnection
          socket.on('close', (hadError) => {
            const finalPeerId = socket._peerNodeId || peerNodeId || 'unknown'; // Get best known ID
            this.log('CONNECTION_CLOSED', { connectionId, peerId: finalPeerId, hadError });
            this.connections.delete(socket);
            // Clean up associated state if needed (sync times, peer versions are cleaned elsewhere or on memory check)
            // If socket had an active _syncBatch, ensure it's cleaned up
             if (socket._syncBatch) {
                 this.log('SYNC_BATCH_CLEANUP_ON_CLOSE', { connectionId, peerId: finalPeerId });
                 // Attempt to close the batch cleanly, ignore errors
                 if (socket._syncBatch && !socket._syncBatch.closed) { // Added check for socket._syncBatch existence
                    try { socket._syncBatch.close(); } catch(e) { /* ignore */ } 
                 }
                 socket._syncBatch = null; // Nullify reference
             }
             // Clear other sync-related properties
             socket._syncProcessedCounter = null;
             socket._syncTotalWrittenCounter = null;
             socket._syncInitialLocalVersion = null;
             socket._syncInitialLocalLastModified = null;
             socket._syncRemoteMeta = null;
          });

          socket.on('error', (err) => {
            const finalPeerId = socket._peerNodeId || peerNodeId || 'unknown';
            this.log('SOCKET_ERROR', { connectionId, peerId: finalPeerId, error: err.message, stack: err.stack });
            this.connections.delete(socket); // Ensure removal on error
            this.stats.errors++;
            // Cleanup state similar to 'close' handler
            if (socket._syncBatch) {
                 this.log('SYNC_BATCH_CLEANUP_ON_ERROR', { connectionId, peerId: finalPeerId });
                 if (socket._syncBatch && !socket._syncBatch.closed) { // Added check for socket._syncBatch existence
                    try { socket._syncBatch.close(); } catch(e) { /* ignore */ } 
                 }
                 socket._syncBatch = null;
             }
             socket._syncProcessedCounter = null;
             socket._syncTotalWrittenCounter = null;
             socket._syncInitialLocalVersion = null;
             socket._syncInitialLocalLastModified = null;
             socket._syncRemoteMeta = null;
          });
        } catch (err) {
           // Error setting up the connection handler itself
           const connId = socket?._connectionId || connectionId || 'setup_failed';
           this.log('CONNECTION_SETUP_ERROR', { connectionId: connId, error: err.message, stack: err.stack });
           this.stats.errors++;
           if (socket && !socket.destroyed) {
               socket.destroy(err); // Attempt to close the problematic socket
           }
           if (socket) this.connections.delete(socket); // Ensure removal
        }
      });

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

  // *** NEW: Centralized Cleanup Function ***
  _cleanupSyncState(socket) {
      if (!socket) return;
      const peerId = socket._peerNodeId || 'unknown';
      // Check if already cleaned
      if (socket._syncBatch === undefined && socket._activeChunkHandlers === undefined) {
          return;
      }

      // Attempt to close batch if it exists and isn't closed
      if (socket._syncBatch && !socket._syncBatch.closed) {
          this.log('SYNC_STATE_CLEANUP_CLOSING_BATCH', { connectionId: socket._connectionId || 'N/A', peerId });
          try {
              socket._syncBatch.close(); // Don't await, fire and forget
          } catch (e) {
              this.log('SYNC_STATE_CLEANUP_BATCH_CLOSE_ERROR', { connectionId: socket._connectionId || 'N/A', peerId, error: e.message });
          }
      }

      // Nullify all sync-related properties
      socket._syncBatch = undefined;
      socket._syncProcessedCounter = undefined;
      socket._syncTotalWrittenCounter = undefined;
      socket._syncInitialLocalVersion = undefined;
      socket._syncInitialLocalLastModified = undefined;
      socket._syncRemoteMeta = undefined;
      socket._activeChunkHandlers = undefined;
      socket._syncReadyToClean = undefined;
      // << REMOVED: No longer clearing _syncNetCountChange or _syncInitialLocalCount >>

      this.log('SYNC_CHUNK_STATE_CLEANED (centralized)', { connectionId: socket._connectionId || 'N/A', peerId });
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
