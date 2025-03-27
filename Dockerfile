FROM node:18-slim

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy source files
COPY phalanx_db.js ./
COPY phalanx_db_rest.js ./

# Create storage directories
RUN mkdir -p /app/p2p-db-storage /app/db_data

# Expose the default port
EXPOSE 8888

# Set up volumes for persistent storage
VOLUME ["/app/p2p-db-storage", "/app/db_data"]

# Copy .env file if it exists
COPY .env* ./

# Start the server
CMD ["node", "phalanx_db_rest.js"] 