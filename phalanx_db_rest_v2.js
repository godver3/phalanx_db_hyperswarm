#!/usr/bin/env node
'use strict';

import dotenv from 'dotenv';
import express from 'express';
import { P2PDBClient } from './hyperbee_phalanx_db_v2.js';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import crypto from 'crypto';

// Load environment variables
dotenv.config();

// ES module equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// REST API Server with V2 P2P Database (Backward Compatible)
class PhalanxDBRestServerV2 {
  constructor(options = {}) {
    this.port = options.port || process.env.PORT || 8888;
    this.app = express();
    this.p2pClient = null;
    
    // --- Centralized Node ID and Storage Management ---
    const storageDir = options.storageDir || path.join(__dirname, 'hyperbee_storage_v2');
    fs.mkdirSync(storageDir, { recursive: true }); // Ensure storage directory exists
    let nodeId = options.nodeId; // Use passed-in ID if it exists

    // Only look for a file if an ID wasn't passed in options
    if (!nodeId) {
      const nodeIdFile = path.join(__dirname, 'phalanx-node-id.json');
      try {
        if (fs.existsSync(nodeIdFile)) {
          const data = JSON.parse(fs.readFileSync(nodeIdFile, 'utf8'));
          if (data.nodeId) {
            nodeId = data.nodeId;
            console.log(`Loaded existing node ID: ${nodeId}`);
          }
        }
      } catch (e) {
        console.error('Could not read node ID from file, will generate a new one.', e);
      }
    }

    // Generate a new ID only if we still don't have one
    if (!nodeId) {
      nodeId = crypto.randomBytes(4).toString('hex');
      const nodeIdFile = path.join(__dirname, 'phalanx-node-id.json');
      try {
        fs.writeFileSync(nodeIdFile, JSON.stringify({ nodeId: nodeId }));
        console.log(`Generated and saved new node ID: ${nodeId}`);
      } catch (e) {
        console.error('Could not write new node ID to file.', e);
      }
    }
    
    // V2 specific options passed to the P2P client
    this.dbOptions = {
      nodeId: nodeId,
      storageDir: storageDir,
      debug: options.debug || process.env.DEBUG === 'true',
      preferNativeReplication: options.preferNativeReplication === true,
      topic: options.topic
    };
    
    // Rate limiting configuration
    this.rateLimits = new Map(); // IP -> { count, resetTime }
    this.maxRequestsPerMinute = parseInt(process.env.RATE_LIMIT) || 600; // 10 requests per second
    
    // Input validation patterns
    this.validationPatterns = {
      infohashService: /^[a-zA-Z0-9]{40}\+[a-zA-Z0-9_\-]+$/,
      infohash: /^[a-zA-Z0-9]{40}$/,
      service: /^[a-zA-Z0-9_\-]+$/
    };
    
    this.setupMiddleware();
    this.setupRoutes();
  }

  // Input validation helper
  validateInput(input, pattern) {
    if (!input || typeof input !== 'string') return false;
    return pattern.test(input);
  }

  // Rate limiting middleware
  rateLimitMiddleware(req, res, next) {
    const ip = req.ip || req.connection.remoteAddress;
    const now = Date.now();
    
    let rateInfo = this.rateLimits.get(ip);
    if (!rateInfo || rateInfo.resetTime < now) {
      rateInfo = { count: 0, resetTime: now + 60000 }; // Reset every minute
      this.rateLimits.set(ip, rateInfo);
    }
    
    rateInfo.count++;
    if (rateInfo.count > this.maxRequestsPerMinute) {
      return res.status(429).json({ 
        error: 'Too many requests', 
        retryAfter: Math.ceil((rateInfo.resetTime - now) / 1000) 
      });
    }
    
    // Clean up old entries periodically
    if (this.rateLimits.size > 10000) {
      for (const [oldIp, info] of this.rateLimits) {
        if (info.resetTime < now) {
          this.rateLimits.delete(oldIp);
        }
      }
    }
    
    next();
  }

  setupMiddleware() {
    this.app.use(express.json({ limit: '50mb' }));
    
    // Rate limiting
    this.app.use((req, res, next) => this.rateLimitMiddleware(req, res, next));
    
    // CORS middleware
    this.app.use((req, res, next) => {
      res.header('Access-Control-Allow-Origin', '*');
      res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
      res.header('Access-Control-Allow-Headers', 'Content-Type, X-Encryption-Key');
      if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
      }
      next();
    });
    
    // Encryption key middleware (backward compatibility)
    const validateEncryptionKey = (req, res, next) => {
      // Skip validation if no encryption key is configured
      if (!process.env.ENCRYPTION_KEY) {
        return next();
      }
      
      const providedKey = req.headers['x-encryption-key'];
      
      if (!providedKey) {
        return res.status(401).json({ error: 'Missing encryption key header' });
      }
      
      if (providedKey !== process.env.ENCRYPTION_KEY) {
        return res.status(403).json({ error: 'Invalid encryption key' });
      }
      
      next();
    };

    // Apply encryption key validation to all API routes
    this.app.use('/api', validateEncryptionKey);
    
    // Request logging
    this.app.use((req, res, next) => {
      console.log(`${new Date().toISOString()} ${req.method} ${req.path}`);
      next();
    });
  }

  setupRoutes() {
    // V1 Compatible API Routes (with /api prefix)
    
    // Get all entries in structured format with pagination (V1 compatible)
    this.app.get('/api/entries', async (req, res) => {
      try {
        // Parse query parameters for pagination (V1 compatible)
        const limit = parseInt(req.query.limit) || 100;
        const startKey = req.query.startKey || req.query.offset || undefined;
        const sortByTime = req.query.sort !== 'key';
        
        // Limit to max 1000 entries per request to prevent timeouts
        const safeLimit = Math.min(limit, 1000);
        
        const structuredEntries = await this.p2pClient.getAllEntriesStructured({
          limit: safeLimit,
          startKey: startKey,
          sortByTime: sortByTime
        });
        
        // Add pagination info to response (V1 compatible)
        const response = {
          ...structuredEntries,
          pagination: {
            limit: safeLimit,
            returned: structuredEntries.data.length,
            sortedBy: sortByTime ? 'timestamp' : 'key'
          }
        };
        
        // Handle pagination differently based on sort method
        if (sortByTime && structuredEntries.totalEntries !== undefined) {
          // Time-based pagination uses offsets
          const currentOffset = parseInt(startKey) || 0;
          response.pagination.offset = currentOffset;
          response.pagination.total = structuredEntries.totalEntries;
          response.pagination.hasMore = structuredEntries.nextKey !== null;
          if (structuredEntries.nextKey) {
            response.pagination.nextOffset = structuredEntries.nextKey;
          }
        } else {
          // Key-based pagination
          response.pagination.hasMore = structuredEntries.data.length === safeLimit;
          if (response.pagination.hasMore && structuredEntries.data.length > 0) {
            const lastEntry = structuredEntries.data[structuredEntries.data.length - 1];
            const lastService = Object.keys(lastEntry.services).pop();
            response.pagination.nextKey = `${lastEntry.infohash}+${lastService}`;
          }
        }
        
        res.json(response);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Get a specific entry (V1 compatible)
    this.app.get('/api/entries/:id', async (req, res) => {
      try {
        // Validate input
        const id = req.params.id;
        if (!id || typeof id !== 'string' || id.length > 100) {
          return res.status(400).json({ error: 'Invalid entry ID' });
        }
        
        // Check if the id contains a service specification
        const [infohash, service] = id.split('+');
        
        if (!service) {
          // If no service specified, get all entries and filter by infohash
          const allEntries = await this.p2pClient.getAllEntriesStructured();
          const matchingEntries = allEntries.data.filter(entry => entry.infohash === infohash);
          
          if (matchingEntries.length > 0) {
            res.json({ data: matchingEntries });
          } else {
            res.status(404).json({ error: 'No entries found for this infohash' });
          }
          return;
        }

        // If service is specified, get that specific entry (existing behavior)
        const entry = await this.p2pClient.getEntry(req.params.id);
        if (entry) {
          const data = [{
            infohash,
            services: {
              [service]: {
                cached: typeof entry.cacheStatus === 'boolean' ? entry.cacheStatus : 
                  (entry.cacheStatus === 'completed' || entry.cacheStatus === 'cached'),
                last_modified: entry.timestamp,
                expiry: entry.expiration,
                // Include V2 features optionally
                ...(entry.vectorClock && { vector_clock: entry.vectorClock }),
                ...(entry.updatedBy && { updated_by: entry.updatedBy })
              }
            }
          }];
          res.json({ data });
        } else {
          res.status(404).json({ error: 'Entry not found' });
        }
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Add new entries (V1 compatible - supports both single entry and structured format)
    this.app.post('/api/entries', async (req, res) => {
      try {
        // Check if it's the structured format
        if (req.body.data && Array.isArray(req.body.data)) {
          const results = await this.p2pClient.addEntriesStructured(req.body);
          res.status(201).json({ results });
        } 
        // Or the single entry format
        else if (req.body.infohashService && req.body.cacheStatus !== undefined) {
          const { infohashService, cacheStatus, expiration } = req.body;
          const result = await this.p2pClient.addEntry(infohashService, cacheStatus, expiration);
          
          if (result) {
            const entry = await this.p2pClient.getEntry(infohashService);
            const [infohash, service] = infohashService.split('+');
            const structuredEntry = {
              data: [{
                infohash,
                services: {
                  [service]: {
                    cached: typeof entry.cacheStatus === 'boolean' ? entry.cacheStatus : 
                      (entry.cacheStatus === 'completed' || entry.cacheStatus === 'cached'),
                    last_modified: entry.timestamp,
                    expiry: entry.expiration
                  }
                }
              }]
            };
            res.status(201).json(structuredEntry);
          } else {
            res.status(500).json({ error: 'Failed to add entry' });
          }
        } else {
          res.status(400).json({ 
            error: 'Invalid request format. Please provide either a structured format with data array or a single entry with infohashService and cacheStatus.'
          });
        }
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Update entries (V1 compatible - supports structured format)
    this.app.put('/api/entries', async (req, res) => {
      try {
        // Check if it's the structured format
        if (req.body.data && Array.isArray(req.body.data)) {
          const results = await this.p2pClient.updateEntriesStructured(req.body);
          res.json({ results });
        } else {
          res.status(400).json({ 
            error: 'Invalid request format. Please provide a structured format with data array.'
          });
        }
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Update a single entry (V1 compatible)
    this.app.put('/api/entries/:id', async (req, res) => {
      try {
        const { cacheStatus, expiration } = req.body;
        const infohashService = req.params.id;
        
        const result = await this.p2pClient.updateEntry(infohashService, cacheStatus, expiration);
        
        if (result) {
          const entry = await this.p2pClient.getEntry(infohashService);
          const [infohash, service] = infohashService.split('+');
          const structuredEntry = {
            data: [{
              infohash,
              services: {
                [service]: {
                  cached: typeof entry.cacheStatus === 'boolean' ? entry.cacheStatus : 
                    (entry.cacheStatus === 'completed' || entry.cacheStatus === 'cached'),
                  last_modified: entry.timestamp,
                  expiry: entry.expiration
                }
              }
            }]
          };
          res.json(structuredEntry);
        } else {
          res.status(404).json({ error: 'Entry not found' });
        }
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Delete an entry (V1 compatible)
    this.app.delete('/api/entries/:id', async (req, res) => {
      try {
        const infohashService = req.params.id;
        
        const result = await this.p2pClient.deleteEntry(infohashService);
        
        if (result) {
          res.json({ success: true });
        } else {
          res.status(404).json({ error: 'Entry not found' });
        }
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Trigger a manual sync (V1 compatible)
    this.app.post('/api/sync', async (req, res) => {
      try {
        // V2 enhanced sync - try delta sync first
        let syncCount = 0;
        for (const [peerId, socket] of this.p2pClient.peers) {
          if (!socket.destroyed && !socket._isLegacy && !socket._isReplicating) {
            socket.write(JSON.stringify({
              type: 'sync_request',
              senderNodeId: this.p2pClient.nodeId,
              timestamp: Date.now()
            }) + '\n');
            syncCount++;
          }
        }
        res.json({ success: true, peersNotified: syncCount });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Get debug info (V1 compatible with V2 enhancements)
    this.app.get('/api/debug', async (req, res) => {
      try {
        const stats = await this.p2pClient.getStats();
        
        // V1 compatible response with V2 enhancements
        const response = {
          ...stats,
          // V2 specific stats
          v2Features: {
            nativeReplications: stats.nativeReplications,
            deltaSyncs: stats.deltaSyncs,
            fullSyncs: stats.fullSyncs,
            conflicts: stats.conflicts,
            vectorClock: stats.vectorClock
          },
          peers: []
        };
        
        // Add peer information
        for (const [peerId, socket] of this.p2pClient.peers) {
          response.peers.push({
            id: peerId,
            connected: !socket.destroyed,
            isNativeReplication: socket._isReplicating || false,
            supportsVectorClocks: socket._supportsVectorClocks || false,
            supportsDeltaSync: socket._supportsDeltaSync || false,
            isLegacy: socket._isLegacy || false
          });
        }
        
        res.json(response);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // V2 Specific API Routes (new functionality)
    
    // Enhanced health check with V2 stats
    this.app.get('/health', async (req, res) => {
      try {
        const stats = await this.p2pClient.getStats();
        const uptime = process.uptime();
        const dbHealthy = this.p2pClient && this.p2pClient.db && this.p2pClient.db.bee;
        
        res.json({
          status: dbHealthy ? 'healthy' : 'degraded',
          version: 'v2',
          uptime: {
            seconds: Math.floor(uptime),
            human: `${Math.floor(uptime / 3600)}h ${Math.floor((uptime % 3600) / 60)}m`
          },
          features: {
            nativeReplication: true,
            vectorClocks: true,
            deltaSync: true,
            backwardCompatible: true,
            rateLimiting: true,
            inputValidation: true,
            syncLogCleanup: true
          },
          database: {
            connected: dbHealthy,
            entries: stats.databaseEntries,
            version: stats.databaseVersion,
            lastModified: stats.databaseLastModified
          },
          network: {
            peers: stats.connectionsActive,
            nodeId: stats.nodeId
          },
          performance: {
            memory: stats.memory,
            requestsPerMinute: this.maxRequestsPerMinute
          }
        });
      } catch (err) {
        res.status(500).json({ 
          status: 'error',
          error: err.message,
          version: 'v2'
        });
      }
    });

    // V2 batch operations endpoint
    this.app.post('/api/batch', async (req, res) => {
      try {
        const { data } = req.body;
        if (!data || !Array.isArray(data)) {
          return res.status(400).json({ error: 'Invalid data format' });
        }
        
        const results = await this.p2pClient.addEntriesStructured({ data });
        
        const successful = results.filter(r => r.success).length;
        const failed = results.filter(r => !r.success).length;
        
        res.json({
          message: 'Batch operation completed',
          successful,
          failed,
          total: results.length,
          node_id: this.p2pClient.nodeId
        });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // V2 sync status endpoint
    this.app.get('/api/sync/status', async (req, res) => {
      try {
        const stats = await this.p2pClient.getStats();
        const peers = [];
        
        for (const [peerId, socket] of this.p2pClient.peers) {
          peers.push({
            id: peerId,
            connected: !socket.destroyed,
            isNativeReplication: socket._isReplicating || false,
            supportsVectorClocks: socket._supportsVectorClocks || false,
            supportsDeltaSync: socket._supportsDeltaSync || false,
            isLegacy: socket._isLegacy || false
          });
        }
        
        res.json({
          local: {
            nodeId: this.p2pClient.nodeId,
            version: this.p2pClient.db.version,
            entries: this.p2pClient.db.activeEntriesCount,
            vectorClock: this.p2pClient.db.vectorClock
          },
          peers,
          stats: {
            nativeReplications: stats.nativeReplications,
            deltaSyncs: stats.deltaSyncs,
            fullSyncs: stats.fullSyncs,
            conflicts: stats.conflicts
          }
        });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // V2 trigger specific sync strategies
    this.app.post('/api/sync/trigger', async (req, res) => {
      try {
        const { strategy = 'auto' } = req.body; // 'auto', 'delta', 'full', 'native'
        
        let syncCount = 0;
        
        for (const [peerId, socket] of this.p2pClient.peers) {
          if (socket.destroyed) continue;
          
          switch (strategy) {
            case 'native':
              if (!socket._isReplicating && socket._supportsNativeReplication) {
                console.log(`Native replication requires reconnection with ${peerId}`);
              }
              break;
              
            case 'delta':
              if (socket._supportsDeltaSync) {
                const lastSync = this.p2pClient.db.syncStates.get(peerId) || 0;
                await this.p2pClient.sendDeltaSync(socket, lastSync);
                syncCount++;
              }
              break;
              
            case 'full':
              await this.p2pClient.sendFullStateOptimized(socket);
              syncCount++;
              break;
              
            case 'auto':
            default:
              socket.write(JSON.stringify({
                type: 'sync_request',
                senderNodeId: this.p2pClient.nodeId,
                timestamp: Date.now()
              }) + '\n');
              syncCount++;
          }
        }
        
        res.json({
          message: 'Sync triggered',
          strategy,
          peersNotified: syncCount
        });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // V2 conflict information
    this.app.get('/api/conflicts', async (req, res) => {
      try {
        const stats = await this.p2pClient.getStats();
        res.json({
          totalConflicts: stats.conflicts,
          message: 'Detailed conflict history available in future version'
        });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });
    
    // Metadata repair endpoint (admin use)
    this.app.post('/api/repair/metadata', async (req, res) => {
      try {
        if (!this.p2pClient.db.bee.core.writable) {
          return res.status(400).json({ error: 'Database is not writable' });
        }
        
        const { fixUpdatedBy = false } = req.body;
        
        const before = {
          version: this.p2pClient.db.version,
          vectorClock: { ...this.p2pClient.db.vectorClock },
          entries: this.p2pClient.db.activeEntriesCount
        };
        
        // Force recalculation of metadata
        const stream = this.p2pClient.db.bee.createReadStream();
        const nodeVectorClocks = {};
        let maxVersion = 0;
        let entriesWithVectorClocks = 0;
        let activeCount = 0;
        let entriesFixed = 0;
        
        const entriesToFix = [];
        
        for await (const { key, value } of stream) {
          if (!key.startsWith(this.p2pClient.db.METADATA_PREFIX) && value) {
            if (!value.deleted) activeCount++;
            
            // Check if updatedBy needs fixing
            if (fixUpdatedBy && value.updatedBy && value.updatedBy.includes(',')) {
              const uniqueNodes = [...new Set(value.updatedBy.split(','))];
              if (uniqueNodes.length === 1 || value.updatedBy.length > 50) {
                entriesToFix.push({ key, value, fixedUpdatedBy: uniqueNodes[0] || this.p2pClient.nodeId });
              }
            }
            
            // Aggregate vector clocks from all entries
            if (value.vectorClock) {
              entriesWithVectorClocks++;
              for (const [nodeId, clock] of Object.entries(value.vectorClock)) {
                nodeVectorClocks[nodeId] = Math.max(nodeVectorClocks[nodeId] || 0, clock);
              }
            }
          }
        }
        
        // Fix entries with problematic updatedBy fields
        if (fixUpdatedBy && entriesToFix.length > 0) {
          const batch = this.p2pClient.db.bee.batch();
          for (const { key, value, fixedUpdatedBy } of entriesToFix) {
            const fixed = { ...value, updatedBy: fixedUpdatedBy };
            await batch.put(key, fixed);
            entriesFixed++;
          }
          await batch.flush();
        }
        
        // Update metadata
        this.p2pClient.db.activeEntriesCount = activeCount;
        
        if (Object.keys(nodeVectorClocks).length > 0) {
          this.p2pClient.db.vectorClock = nodeVectorClocks;
        }
        
        // Ensure our node has a proper vector clock value
        if (!this.p2pClient.db.vectorClock[this.p2pClient.nodeId] || 
            this.p2pClient.db.vectorClock[this.p2pClient.nodeId] === 0) {
          this.p2pClient.db.vectorClock[this.p2pClient.nodeId] = Math.max(
            activeCount,
            Object.values(nodeVectorClocks).reduce((max, val) => Math.max(max, val), 0)
          );
        }
        
        this.p2pClient.db.version = Math.max(
          this.p2pClient.db.version,
          this.p2pClient.db.vectorClock[this.p2pClient.nodeId]
        );
        
        // Persist the repaired metadata
        await this.p2pClient.db._updateMetadata(false);
        
        const after = {
          version: this.p2pClient.db.version,
          vectorClock: { ...this.p2pClient.db.vectorClock },
          entries: this.p2pClient.db.activeEntriesCount
        };
        
        res.json({
          message: 'Metadata repaired',
          before,
          after,
          entriesScanned: activeCount,
          entriesWithVectorClocks,
          entriesFixed: fixUpdatedBy ? entriesFixed : undefined
        });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });
  }

  async start() {
    try {
      // Initialize P2P client with pre-determined options
      this.p2pClient = new P2PDBClient(this.dbOptions);
      await this.p2pClient.start();
      
      console.log('V2 P2P network started (Backward Compatible)');
      console.log('Features: Native replication, Vector clocks, Delta sync, V1 API compatibility');
      
      // Start HTTP server
      this.server = this.app.listen(this.port, () => {
        console.log(`REST API (V2) listening on port ${this.port}`);
        console.log(`Node ID: ${this.p2pClient.nodeId}`);
        console.log(`Debug info available at http://localhost:${this.port}/api/debug`);
        console.log(`List entries at http://localhost:${this.port}/api/entries`);
        console.log(`V2 features at http://localhost:${this.port}/health`);
      });
      
      // Graceful shutdown
      process.on('SIGINT', () => this.stop());
      process.on('SIGTERM', () => this.stop());
      
    } catch (err) {
      console.error('Failed to start server:', err);
      process.exit(1);
    }
  }

  async stop() {
    console.log('\nShutting down...');
    
    if (this.server) {
      this.server.close();
    }
    
    if (this.p2pClient) {
      await this.p2pClient.stop();
    }
    
    process.exit(0);
  }
}

// Parse command line arguments
const args = process.argv.slice(2);
const options = {
  port: undefined, // Will use process.env.PORT or 8888
  debug: false,
  preferNativeReplication: false, // Default false - use explicit --native flag to enable
  seedHex: null,
  writerKey: null,
  isWriter: true
};

for (let i = 0; i < args.length; i++) {
  switch (args[i]) {
    case '--port':
      options.port = parseInt(args[++i]);
      break;
    case '--node-id':
      options.nodeId = args[++i];
      break;
    case '--storage':
      options.storageDir = args[++i];
      break;
    case '--topic':
      options.topic = args[++i];
      break;
    case '--debug':
      options.debug = true;
      break;
    case '--native':
      options.preferNativeReplication = true;
      break;
    case '--seed':
      options.seedHex = args[++i];
      break;
    case '--writer-key':
      options.writerKey = args[++i];
      options.isWriter = false; // If writer-key is provided, this is a reader
      break;
    case '--help':
      console.log('Usage: node phalanx_db_rest_v2.js [options]');
      console.log('\nOptions:');
      console.log('  --port <port>       Port to listen on (default: process.env.PORT or 8888)');
      console.log('  --node-id <id>      Node ID for P2P network');
      console.log('  --storage <dir>     Storage directory');
      console.log('  --topic <topic>     P2P topic name');
      console.log('  --debug             Enable debug logging');
      console.log('  --native            Enable native replication (disabled by default)');
      console.log('  --seed <seedHex>    Seed hex for deterministic key generation (writer mode)');
      console.log('  --writer-key <key>  Hypercore key to replicate from (reader mode)');
      console.log('  --help              Show this help message');
      console.log('\nEnvironment Variables:');
      console.log('  ENCRYPTION_KEY      Optional encryption key for API security');
      console.log('  DEBUG               Enable debug mode (true/false)');
      console.log('  PORT                Port to listen on');
      console.log('  RATE_LIMIT          Max requests per minute per IP (default: 600)');
      console.log('\nProduction Features:');
      console.log('  - Rate limiting (configurable via RATE_LIMIT)');
      console.log('  - Input validation and sanitization');
      console.log('  - Automatic sync log cleanup');
      console.log('  - Connection retry logic');
      console.log('  - Memory-efficient pagination');
      console.log('  - Vector clock conflict resolution');
      console.log('  - Delta sync optimization');
      process.exit(0);
  }
}

// Start the server
const server = new PhalanxDBRestServerV2(options);
server.start(); 