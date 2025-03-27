#!/usr/bin/env node
'use strict';

require('dotenv').config();
const express = require('express');
const { P2PDBClient } = require('./phalanx_db');

// Create an Express app
const app = express();
app.use(express.json({ limit: '50mb' }));

// Encryption key middleware
const validateEncryptionKey = (req, res, next) => {
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
app.use('/api', validateEncryptionKey);

// Create a P2P database client
const client = new P2PDBClient({
  debug: process.env.DEBUG === 'true'
});

// Start the P2P network
client.start().catch(console.error);

// API routes
// Get all entries in structured format
app.get('/api/entries', async (req, res) => {
  try {
    const structuredEntries = await client.getAllEntriesStructured();
    res.json(structuredEntries);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get a specific entry
app.get('/api/entries/:id', async (req, res) => {
  try {
    // Check if the id contains a service specification
    const [infohash, service] = req.params.id.split('+');
    
    if (!service) {
      // If no service specified, get all entries and filter by infohash
      const allEntries = await client.getAllEntriesStructured();
      const matchingEntries = allEntries.data.filter(entry => entry.infohash === infohash);
      
      if (matchingEntries.length > 0) {
        res.json({ data: matchingEntries });
      } else {
        res.status(404).json({ error: 'No entries found for this infohash' });
      }
      return;
    }

    // If service is specified, get that specific entry (existing behavior)
    const entry = await client.getEntry(req.params.id);
    if (entry) {
      const data = [{
        infohash,
        services: {
          [service]: {
            cached: typeof entry.cacheStatus === 'boolean' ? entry.cacheStatus : 
              (entry.cacheStatus === 'completed' || entry.cacheStatus === 'cached'),
            last_modified: entry.timestamp,
            expiry: entry.expiration
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

// Add new entries (supports both single entry and structured format)
app.post('/api/entries', async (req, res) => {
  try {
    // Check if it's the structured format
    if (req.body.data && Array.isArray(req.body.data)) {
      const results = await client.addEntriesStructured(req.body);
      res.status(201).json({ results });
    } 
    // Or the single entry format
    else if (req.body.infohashService && req.body.cacheStatus !== undefined) {
      const { infohashService, cacheStatus, expiration } = req.body;
      const result = await client.addEntry(infohashService, cacheStatus, expiration);
      
      if (result) {
        const entry = await client.getEntry(infohashService);
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

// Update entries (supports both single entry and structured format)
app.put('/api/entries', async (req, res) => {
  try {
    // Check if it's the structured format
    if (req.body.data && Array.isArray(req.body.data)) {
      const results = await client.updateEntriesStructured(req.body);
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

// Update a single entry
app.put('/api/entries/:id', async (req, res) => {
  try {
    const { cacheStatus, expiration } = req.body;
    const infohashService = req.params.id;
    
    const result = await client.updateEntry(infohashService, cacheStatus, expiration);
    
    if (result) {
      const entry = await client.getEntry(infohashService);
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

// Delete an entry
app.delete('/api/entries/:id', async (req, res) => {
  try {
    const infohashService = req.params.id;
    
    const result = await client.deleteEntry(infohashService);
    
    if (result) {
      res.json({ success: true });
    } else {
      res.status(404).json({ error: 'Entry not found' });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Trigger a manual sync
app.post('/api/sync', async (req, res) => {
  try {
    await client.syncWithPeers();
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get debug info
app.get('/api/debug', async (req, res) => {
  try {
    const stats = await client.getStats();
    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Set up the server
const PORT = process.env.PORT || 8888;
app.listen(PORT, () => {
  console.log(`REST API server running on http://localhost:${PORT}`);
  console.log(`Debug info available at http://localhost:${PORT}/api/debug`);
  console.log(`List entries at http://localhost:${PORT}/api/entries`);
});

// Handle shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down server...');
  await client.stop();
  process.exit();
}); 