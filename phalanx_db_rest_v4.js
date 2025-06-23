#!/usr/bin/env node
'use strict';

import dotenv from 'dotenv';
import express from 'express';
import { P2PDBClient } from './hyperbee_phalanx_db_v4.js';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

class PhalanxDBRestServerV4 {
  constructor(options = {}) {
    this.port = options.port || process.env.PORT || 8888; // New default port for v4
    this.app = express();
    this.p2pClient = new P2PDBClient({
        storageDir: options.storageDir,
        topic: options.topic
    });
    
    this.setupMiddleware();
    this.setupRoutes();
  }

  setupMiddleware() {
    this.app.use(express.json({ limit: '50mb' }));
    
    this.app.use((req, res, next) => {
      res.header('Access-Control-Allow-Origin', '*');
      res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
      res.header('Access-Control-Allow-Headers', 'Content-Type');
      if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
      }
      next();
    });
    
    this.app.use((req, res, next) => {
      console.log(`${new Date().toISOString()} ${req.method} ${req.path}`);
      next();
    });
  }

  setupRoutes() {
    this.app.get('/health', async (req, res) => {
      res.json({
        status: 'healthy',
        version: 'v4',
        localWriterKey: this.p2pClient.writer.key.toString('hex'),
        view: {
          version: this.p2pClient.view.version,
          byteLength: this.p2pClient.view.byteLength
        },
        knownWriters: Array.from(this.p2pClient.knownWriters),
        swarm: {
            peers: this.p2pClient.swarm.peers.size,
            topic: this.p2pClient.topicString
        }
      });
    });

    // Get all entries
    this.app.get('/api/entries', async (req, res) => {
      try {
        const allEntries = await this.p2pClient.list();
        res.json({ data: this.toStructuredFormat(allEntries) });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Get a specific entry or all entries for an infohash
    this.app.get('/api/entries/:id', async (req, res) => {
      try {
        const id = req.params.id;
        const [infohash, service] = id.split('+');

        if (!service) {
          const allEntries = await this.p2pClient.list();
          const matchingEntries = allEntries.filter(e => e.key.startsWith(infohash + '+'));
          if (matchingEntries.length > 0) {
            res.json({ data: this.toStructuredFormat(matchingEntries) });
          } else {
            res.status(404).json({ error: 'No entries found for this infohash' });
          }
        } else {
          const result = await this.p2pClient.get(id);
          if (result) {
            res.json({ data: this.toStructuredFormat([result]) });
          } else {
            res.status(404).json({ error: 'Entry not found' });
          }
        }
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Add/Update new entries
    this.app.post('/api/entries', async (req, res) => {
      try {
        if (req.body.data && Array.isArray(req.body.data)) {
          for (const item of req.body.data) {
            const infohash = item.infohash;
            for (const [service, serviceData] of Object.entries(item.services)) {
              const key = `${infohash}+${service}`;
              const value = {
                cached: serviceData.cached,
                last_modified: new Date().toISOString(),
                expiry: serviceData.expiry,
              };
              await this.p2pClient.put(key, value);
            }
          }
          res.status(201).json({ success: true, message: 'Entries created' });
        } else {
          res.status(400).json({
            error: 'Invalid request format. Please provide a structured format with data array.'
          });
        }
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    this.app.put('/api/entries', async (req, res) => {
      try {
        if (req.body.data && Array.isArray(req.body.data)) {
          for (const item of req.body.data) {
            const infohash = item.infohash;
            for (const [service, serviceData] of Object.entries(item.services)) {
              const key = `${infohash}+${service}`;
              const value = {
                cached: serviceData.cached,
                last_modified: new Date().toISOString(),
                expiry: serviceData.expiry,
              };
              await this.p2pClient.put(key, value);
            }
          }
          res.status(200).json({ success: true, message: 'Entries updated' });
        } else {
          res.status(400).json({
            error: 'Invalid request format. Please provide a structured format with data array.'
          });
        }
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Delete an entry
    this.app.delete('/api/entries/:id', async (req, res) => {
      try {
        const infohashService = req.params.id;
        await this.p2pClient.del(infohashService);
        res.json({ success: true });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Get debug info (V4 implementation, follows V2 structure)
    this.app.get('/api/debug', async (req, res) => {
      try {
        const stats = await this.p2pClient.getStats();
        
        const peers = [];
        for (const conn of this.p2pClient.swarm.connections) {
          if (conn.remotePublicKey) {
            peers.push({
              id: conn.remotePublicKey.toString('hex'),
              type: conn.type,
              client: conn.client
            });
          }
        }

        const toMB = (bytes) => (bytes / 1024 / 1024).toFixed(2);
        
        const formattedMemory = {
          rss: toMB(stats.memory.rss) + ' MB',
          heapTotal: toMB(stats.memory.heapTotal) + ' MB',
          heapUsed: toMB(stats.memory.heapUsed) + ' MB',
          external: toMB(stats.memory.external) + ' MB',
          arrayBuffers: toMB(stats.memory.arrayBuffers) + ' MB',
        };

        const response = {
          ...stats,
          memory: formattedMemory,
          v4Features: {
            architecture: 'Multi-writer Hypercore logs with Hyperbee view',
            discovery: 'Hyperswarm RPC'
          },
          peers
        };
        
        res.json(response);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });
  }

  // Helper to convert flat entries to the legacy structured format
  toStructuredFormat(entries) {
    const infohashMap = new Map();
    for (const { key, value } of entries) {
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
          infohashMap.set(infohash, infohashEntry);
        }
        
        infohashEntry.services[service] = {
            cached: value.cached,
            last_modified: value.last_modified,
            expiry: value.expiry
        };
    }
    return Array.from(infohashMap.values());
  }

  async start() {
    try {
      await this.p2pClient.start();
      
      this.server = this.app.listen(this.port, () => {
        console.log(`REST API (V4) listening on port ${this.port}`);
        console.log(`Health info available at http://localhost:${this.port}/health`);
      });
      
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

const args = process.argv.slice(2);
const options = {};

for (let i = 0; i < args.length; i++) {
  switch (args[i]) {
    case '--port':
      options.port = parseInt(args[++i]);
      break;
    case '--storage':
      options.storageDir = args[++i];
      break;
    case '--topic':
      options.topic = args[++i];
      break;
    case '--help':
      console.log('Usage: node phalanx_db_rest_v4.js [options]');
      console.log('\nOptions:');
      console.log('  --port <port>       Port to listen on (default: 8888)');
      console.log('  --storage <dir>     Storage directory for v4');
      console.log('  --topic <topic>     P2P topic name');
      process.exit(0);
  }
}

const server = new PhalanxDBRestServerV4(options);
server.start(); 