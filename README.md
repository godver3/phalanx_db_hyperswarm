# PhalanxDB - Distributed database build on Hyperswarm

A Node.js implementation with Express server integration built on Hyperswarm. PhalanxDB is designed for integration with cli_debrid and Riven, and is likely not useful in any other context.

## Features

- Hyperswarm peer-to-peer database integration
- Multi-service support: track the same infohash across different services
- Cache management:
  - Two-state cache system: 'true' and 'false'
  - Automatic expiry calculation when not explicitly provided:
    - Cached items expire after 7 days from their last modification
    - Uncached items expire after 24 hours from their last modification
  - Cache status and expiry dates are preserved as provided

## API Endpoints

All endpoints require authentication using a Bearer token. The encryption key should be provided in the Authorization header.

### Add Data

```bash
curl -X POST http://localhost:8888/data \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "infohash": "example_hash_123",
    "cached": true,
    "service": "real_debrid",
    "last_modified": "2024-03-12T12:00:00Z",
    "expiry": "2024-03-19T12:00:00Z"
  }'
```

**Data Validation Requirements:**
- Required fields:
  - `infohash`: must be a string
  - `service`: must be a string
    - Should typically be formatted as per the following list:
      - real_debrid
      - premiumize
      - easy_debrid
      - torbox
    - Services will be normalized regardless to lowercase replacing spaces with underscores
  - `cached`: must be a boolean (true or false)
- Optional fields:
  - `last_modified`: if provided, must be a valid ISO 8601 timestamp with Z suffix (UTC time)
  - `expiry`: if provided, must be a valid ISO 8601 timestamp with Z suffix (UTC time)
- Requests missing required fields or containing invalid data types will be rejected

Note: All timestamps must be in UTC format (ISO 8601) with the 'Z' suffix indicating UTC timezone (e.g., "2024-03-12T12:00:00Z")

Note: No actual expiration logic is applied - in otherwise once added data is static. Expiration date and time can be used by clients to determine whether or not to consider the data valid

### Get Specific Data by Infohash

```bash
# Get data for a specific infohash (across all services)
curl http://localhost:8888/data/example_hash_123 \
  -H "Authorization: Bearer TOKEN"

# Get data for a specific infohash and service
curl http://localhost:8888/data/example_hash_123?service=real_debrid \
  -H "Authorization: Bearer TOKEN"
```

Example Responses:

1. When querying by infohash alone (all services):
```json
{
  "total": 2,
  "data": [
    {
      "infohash": "example_hash_123",
      "services": {
        "real_debrid": {
          "cached": true,
          "last_modified": "2024-03-12T12:00:00Z",
          "expiry": "2024-03-19T12:00:00Z"
        },
        "premiumize": {
          "cached": false,
          "last_modified": "2024-03-12T11:00:00Z",
          "expiry": "2024-03-13T11:00:00Z"
        }
      }
    }
  ],
  "schema_version": "2.0"
}
```

2. When querying with a specific service:
```json
{
  "total": 1,
  "data": [
    {
      "infohash": "example_hash_123",
      "services": {
        "real_debrid": {
          "cached": true,
          "last_modified": "2024-03-12T12:00:00Z",
          "expiry": "2024-03-19T12:00:00Z"
        }
      }
    }
  ],
  "schema_version": "2.0"
}
```

Notes:
- When querying by infohash alone, you'll receive all entries for that infohash across different services
- When specifying a service parameter, you'll receive only the matching entry

### Invalidate Data

```bash
# Invalidate all data for a specific infohash
curl -X DELETE http://localhost:8888/data/example_hash_123 \
  -H "Authorization: Bearer TOKEN"

# Invalidate data for a specific infohash and service
curl -X DELETE "http://localhost:8888/data/example_hash_123?service=real_debrid" \
  -H "Authorization: Bearer TOKEN"
```

Notes:
- When invalidating without a service parameter, all data for the infohash is removed
- When specifying a service, only that service's data is removed while preserving other services
- If removing a service leaves no remaining services, the entire infohash entry is removed

### Debug Endpoint

```bash
curl http://localhost:8888/debug \
  -H "Authorization: Bearer TOKEN"
```

The TOKEN should be set to the value of `ENCRYPTION_KEY` from the `.env` file. 

## Links

- https://github.com/rivenmedia/riven
- https://github.com/godver3/cli_debrid
- https://github.com/amark/gun