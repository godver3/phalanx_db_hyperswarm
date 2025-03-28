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
        return;
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

  // Add an entry
  async addEntry(infohashService, cacheStatus, expiration) {
    if (!this.db) {
      throw new Error('Database not initialized');
    }

    try {
      await this.db.put(infohashService, {
        cacheStatus,
        timestamp: new Date().toISOString(),
        expiration: expiration || new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(),
        updatedBy: this.nodeId,
        deleted: false
      });
      this.version++;
      this.lastModified = Date.now();
      return true;
    } catch (err) {
      console.error('Error adding entry:', err);
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
      if (!existingEntry) {
        return false;
      }

      await this.db.put(infohashService, {
        ...existingEntry,
        cacheStatus: cacheStatus || existingEntry.cacheStatus,
        timestamp: new Date().toISOString(),
        expiration: expiration || existingEntry.expiration,
        updatedBy: this.nodeId
      });
      
      this.version++;
      this.lastModified = Date.now();
      return true;
    } catch (err) {
      console.error('Error updating entry:', err);
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

      await this.db.put(infohashService, {
        ...existingEntry,
        deleted: true,
        timestamp: new Date().toISOString(),
        updatedBy: this.nodeId
      });
      
      this.version++;
      this.lastModified = Date.now();
      return true;
    } catch (err) {
      console.error('Error deleting entry:', err);
      return false;
    }
  }

  // Merge with another database
  async merge(otherDb) {
    if (!this.db) {
      throw new Error('Database not initialized');
    }

    let changes = 0;
    
    try {
      // Only apply changes if the other DB has a newer version
      if (otherDb.version <= this.version && otherDb.lastModified <= this.lastModified) {
        return changes;
      }

      // Get entries from both databases
      const otherEntries = otherDb.entries || await otherDb._getAllEntries();
      const currentEntries = await this._getAllEntries();
      
      // Merge entries based on timestamp - but respect tombstones
      const batch = this.db.batch();
      
      for (const [key, entry] of Object.entries(otherEntries)) {
        const existingEntry = currentEntries[key];
        
        if (!existingEntry || new Date(existingEntry.timestamp) < new Date(entry.timestamp)) {
          batch.put(key, entry);
          changes++;
        }
      }
      
      if (changes > 0) {
        await batch.write();
        this.version = Math.max(this.version, otherDb.version) + 1;
        this.lastModified = Date.now();
      }
      
      return changes;
    } catch (err) {
      console.error('Error during merge:', err);
      return 0;
    }
  }

  // Export as JSON
  async toJSON() {
    if (!this.db) {
      throw new Error('Database not initialized');
    }

    const entries = await this._getAllEntries();
    return {
      entries,
      version: this.version,
      lastModified: this.lastModified,
      nodeId: this.nodeId
    };
  }

  // Create from JSON
  static async fromJSON(json, sourceNodeId) {
    const db = new P2PDatabase(sourceNodeId || json.nodeId || nodeId);
    try {
      await db.open();
      
      const entries = json.entries || {};
      
      // Batch write all entries
      const batch = db.db.batch();
      for (const [key, value] of Object.entries(entries)) {
        if (key !== '__test__') { // Skip test key
          batch.put(key, value);
        }
      }
      await batch.write();
      
      db.version = json.version || 0;
      db.lastModified = json.lastModified || Date.now();
      return db;
    } catch (err) {
      console.error('Error creating database from JSON:', err);
      if (db.db) {
        await db.close().catch(console.error);
      }
      throw err;
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
      lastChangeAt: null
    };

    // Track sync chains
    this.activeSyncs = new Map(); // Track active syncs by peer

    // Add sync control
    this.lastSyncTime = new Map(); // Track last sync time per peer
    this.MIN_SYNC_INTERVAL = 1000; // Minimum ms between syncs with same peer
    this.syncInProgress = new Set(); // Track ongoing syncs
    this.MAX_SYNC_CHAIN = 3; // Maximum length of sync chain

    // Add memory management
    this.BUFFER_LIMIT = 100000; // 100KB limit per socket buffer
    this.CLEANUP_INTERVAL = 60000; // Run cleanup every minute
    this.lastCleanup = Date.now();
    
    // Track socket buffers
    this.socketBuffers = new WeakMap();
    
    // Cache database state
    this.cachedState = null;
    this.cacheTimeout = null;
    this.CACHE_DURATION = 1000; // Cache state for 1 second
  }
  
  async initializeDatabase() {
    try {
      this.db = await this.loadDatabase();
      console.log('Database initialized with version:', this.db.version);
      await this.saveDatabase();
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
  
  // Function to save database to disk
  async saveDatabase() {
    const data = JSON.stringify(await this.db.toJSON(), null, 2);
    fs.writeFileSync(this.getSaveFilePath(), data);
    console.log('Saved database to disk');
  }
  
  // Function to load database from disk
  async loadDatabase() {
    try {
      if (fs.existsSync(this.getSaveFilePath())) {
        const data = fs.readFileSync(this.getSaveFilePath(), 'utf8');
        return await P2PDatabase.fromJSON(JSON.parse(data), this.nodeId);
      }
    } catch (err) {
      console.error('Error loading database:', err);
    }
    
    const db = new P2PDatabase(this.nodeId);
    await db.open();
    return db;
  }
  
  // Function to list all entries
  async listAllEntries() {

    
    const entries = await this.db._getAllEntries();

    console.log('Last modified:', new Date(this.db.lastModified).toISOString());

    
    return entries;
  }
  
  // Function to handle sync with a new peer
  async handleSyncRequest(socket, trigger = 'unknown') {
    try {
      const peerId = socket._peerNodeId || 'unknown';
      
      // Prevent sync loops and throttle syncs
      if (this.syncInProgress.has(peerId)) {
        this.log('SYNC_SKIP', {
          peerId,
          reason: 'sync_in_progress'
        });
        return;
      }

      // Check sync chain length
      const syncChain = socket._syncChain || [];
      if (syncChain.includes(this.nodeId) || syncChain.length >= this.MAX_SYNC_CHAIN) {
        this.log('SYNC_SKIP', {
          peerId,
          reason: 'chain_limit',
          chainLength: syncChain.length
        });
        return;
      }

      // Enforce minimum time between syncs
      const now = Date.now();
      const lastSync = this.lastSyncTime.get(peerId) || 0;
      if (now - lastSync < this.MIN_SYNC_INTERVAL) {
        this.log('SYNC_SKIP', {
          peerId,
          reason: 'rate_limit',
          timeSinceLastSync: now - lastSync
        });
        return;
      }

      // Track sync start
      this.syncInProgress.add(peerId);
      this.lastSyncTime.set(peerId, now);
      socket._syncChain = [...syncChain, this.nodeId];
      
      this.log('SYNC_START', {
        peerId,
        trigger,
        syncChain: socket._syncChain,
        activeConnections: this.connections.size
      });
      
      // Get current database state
      const dbState = await this.db.toJSON();
      
      // Add our nodeId and version info to the message
      const message = JSON.stringify({
        ...dbState,
        senderNodeId: this.nodeId,
        syncChain: socket._syncChain
      });
      
      socket.write(message);
      this.stats.syncsSent++;
      this.stats.lastSyncAt = new Date().toISOString();
      
      this.log('SYNC_SENT', {
        peerId,
        trigger,
        dataSize: message.length,
        syncChain: socket._syncChain
      });
      
      // Only request state back if this wasn't a response to their sync
      if (!trigger.startsWith('peer_request_')) {
        setTimeout(() => {
          if (!socket.destroyed) {
            socket.write(JSON.stringify({ 
              requestSync: true,
              senderNodeId: this.nodeId,
              trigger: 'response_request',
              syncChain: socket._syncChain
            }));
            
            this.log('SYNC_REQUEST', {
              peerId,
              trigger: 'response_request',
              syncChain: socket._syncChain
            });
          }
        }, 500);
      }
    } catch (err) {
      this.log('SYNC_ERROR', {
        error: err.message,
        stack: err.stack,
        trigger
      });
      this.stats.errors++;
    } finally {
      // Clear sync state
      if (socket._peerNodeId) {
        this.syncInProgress.delete(socket._peerNodeId);
      }
    }
  }
  
  // Get database state with caching
  async getDatabaseState() {
    const now = Date.now();
    if (this.cachedState && now - this.lastStateCache < this.CACHE_DURATION) {
      return this.cachedState;
    }
    
    const state = await this.db.toJSON();
    this.cachedState = state;
    this.lastStateCache = now;
    return state;
  }

  // Clean up resources
  async cleanup() {
    try {
      const now = Date.now();
      
      // Clear old sync tracking
      for (const [peer, time] of this.lastSyncTime.entries()) {
        if (now - time > 300000) { // 5 minutes
          this.lastSyncTime.delete(peer);
        }
      }
      
      // Clear sync in progress for disconnected peers
      for (const peer of this.syncInProgress) {
        if (!Array.from(this.connections).some(s => s._peerNodeId === peer)) {
          this.syncInProgress.delete(peer);
        }
      }
      
      // Clear cached state if too old
      if (this.cachedState && now - this.lastStateCache > this.CACHE_DURATION) {
        this.cachedState = null;
      }
      
      this.lastCleanup = now;
      
      // Force garbage collection if available
      if (global.gc) {
        global.gc();
      }
    } catch (err) {
      this.log('CLEANUP_ERROR', {
        error: err.message,
        stack: err.stack
      });
    }
  }

  // Handle incoming data from peers with better buffer management
  async handlePeerData(socket, _, data) {
    try {
      // Get or create buffer for this socket
      let buffer = this.socketBuffers.get(socket) || '';
      buffer += data.toString('utf8');
      
      // Check buffer size before processing
      if (buffer.length > this.BUFFER_LIMIT) {
        this.log('BUFFER_OVERFLOW', {
          peerId: socket._peerNodeId || 'unknown',
          bufferSize: buffer.length
        });
        buffer = '';
        return;
      }
      
      try {
        const parsed = JSON.parse(buffer);
        // Clear buffer on successful parse
        buffer = '';
        
        const peerId = parsed.senderNodeId || 'unknown';
        socket._peerNodeId = peerId;
        socket._syncChain = parsed.syncChain || [];
        
        this.stats.syncsReceived++;
        
        this.log('PEER_DATA', {
          peerId,
          messageType: parsed.requestSync ? 'sync_request' : 'data',
          syncChain: socket._syncChain
        });
        
        // Check if this is a sync request
        if (parsed.requestSync) {
          const trigger = parsed.trigger || 'request';
          await this.handleSyncRequest(socket, `peer_request_${trigger}`);
          return;
        }
        
        // Instead of creating a new database instance, just merge the data directly
        const changes = await this.db.merge({
          entries: parsed.entries,
          version: parsed.version,
          lastModified: parsed.lastModified,
          nodeId: peerId
        });
        
        this.log('MERGE_RESULT', {
          peerId,
          changes,
          syncChain: socket._syncChain
        });
        
        if (changes > 0) {
          await this.saveDatabase();
          await this.listAllEntries();
          
          this.stats.changesMerged += changes;
          this.stats.lastChangeAt = new Date().toISOString();
          
          // Only notify a subset of peers about updates to prevent sync storms
          const otherPeers = Array.from(this.connections)
            .filter(s => s !== socket && !s.destroyed);
          
          // Select up to 3 random peers to notify
          const peersToNotify = otherPeers
            .sort(() => Math.random() - 0.5)
            .slice(0, 3);
          
          this.log('NOTIFY_PEERS', {
            peerId,
            selectedPeers: peersToNotify.length,
            totalPeers: otherPeers.length,
            changes,
            syncChain: socket._syncChain
          });
          
          for (const otherSocket of peersToNotify) {
            const otherPeerId = otherSocket._peerNodeId || 'unknown';
            await this.handleSyncRequest(otherSocket, `changes_from_${peerId}`);
          }
        }
      } catch (parseErr) {
        // Update buffer for next chunk
        this.socketBuffers.set(socket, buffer);
      }
    } catch (err) {
      this.log('PEER_DATA_ERROR', {
        error: err.message,
        stack: err.stack
      });
      this.stats.errors++;
      // Clear buffer on error
      this.socketBuffers.delete(socket);
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
      
      // Handle new connections with proper cleanup
      this.swarm.on('connection', (socket, info) => {
        try {
          this.connections.add(socket);
          this.stats.connectionsTotal++;
          
          this.log('NEW_CONNECTION', {
            peerId: socket._peerNodeId || 'unknown',
            peerInfo: info
          });
          
          // Initial sync
          this.handleSyncRequest(socket, 'initial_connection').catch(err => {
            this.log('INITIAL_SYNC_ERROR', {
              error: err.message,
              stack: err.stack
            });
            this.stats.errors++;
          });
          
          // Setup socket data handling
          socket.on('data', data => {
            this.handlePeerData(socket, null, data).catch(err => {
              this.log('DATA_HANDLER_ERROR', {
                error: err.message,
                stack: err.stack
              });
              this.stats.errors++;
            });
          });
          
          // Cleanup on socket close
          socket.on('close', () => {
            if (socket._peerNodeId) {
              this.log('PEER_DISCONNECTED', {
                peerId: socket._peerNodeId
              });
            }
            this.connections.delete(socket);
            this.socketBuffers.delete(socket);
            this.syncInProgress.delete(socket._peerNodeId);
          });
          
          socket.on('error', (err) => {
            this.log('SOCKET_ERROR', {
              error: err.message,
              peerId: socket._peerNodeId
            });
            this.connections.delete(socket);
            this.socketBuffers.delete(socket);
            this.syncInProgress.delete(socket._peerNodeId);
            this.stats.errors++;
          });
          
        } catch (err) {
          this.log('CONNECTION_HANDLER_ERROR', {
            error: err.message,
            stack: err.stack
          });
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
          //this.syncWithPeers();
        }
      }, 30000);
      
      // Add periodic cleanup
      this.cleanupInterval = setInterval(() => {
        this.cleanup().catch(err => {
          this.log('CLEANUP_INTERVAL_ERROR', {
            error: err.message,
            stack: err.stack
          });
        });
      }, this.CLEANUP_INTERVAL);
      
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
  
  // Stop the P2P network with proper cleanup
  async stop() {
    try {
      console.log('\nClosing...');
      
      if (this.syncInterval) {
        clearInterval(this.syncInterval);
      }
      
      if (this.cleanupInterval) {
        clearInterval(this.cleanupInterval);
      }
      
      // Clear all buffers and tracking
      for (const socket of this.connections) {
        this.socketBuffers.delete(socket);
        socket.destroy();
      }
      
      this.connections.clear();
      this.syncInProgress.clear();
      this.lastSyncTime.clear();
      this.cachedState = null;
      
      // Save final state and close database
      await this.saveDatabase();
      await this.db.close();
      
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
        await this.saveDatabase();
        await this.listAllEntries();
        
        // Notify peers of the change
        //this.syncWithPeers();
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
        await this.saveDatabase();
        await this.listAllEntries();
        
        // Notify peers of the change
        //this.syncWithPeers();
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
        await this.saveDatabase();
        await this.listAllEntries();
        
        // Notify peers of the change
        //this.syncWithPeers();
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