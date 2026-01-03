# Cassandra gRPC Weather System

A distributed weather data ingestion and querying system built with Apache Cassandra, gRPC, and Apache Spark. This system processes NOAA (National Oceanic and Atmospheric Administration) weather data from weather stations and provides fault-tolerant data storage and retrieval capabilities.

## Overview

This project implements a gRPC server that receives weather data and inserts it into a 3-node Cassandra cluster. The system is designed for high write availability while maintaining read consistency, making it suitable for sensor data collection scenarios where continuous data ingestion is critical.

## Architecture

### Technology Stack

- **Apache Cassandra 5.0.0**: Distributed NoSQL database with 3-node cluster configuration
- **gRPC**: High-performance RPC framework for client-server communication
- **Apache Spark 3.5.5**: Data preprocessing and ETL operations
- **Python 3**: Primary programming language
- **Docker**: Container orchestration with Docker Compose

### System Design

- **Replication Factor**: 3 (RF=3)
- **Write Consistency**: ONE (CL W=1) for high write availability
- **Read Consistency**: THREE (CL R=3) to satisfy R + W > RF, ensuring strong consistency
- **Schema Design**: Partition key on station ID, clustering key on date, static columns for station metadata

## Features

### Core Functionality

- **Station Metadata Management**: Loads and stores weather station information from NOAA ghcnd-stations.txt file
- **Temperature Recording**: Ingests daily temperature readings (tmin, tmax) for weather stations
- **Station Lookup**: Retrieves station names by station ID
- **Max Temperature Query**: Calculates maximum recorded temperature for a given station
- **Schema Inspection**: Provides Cassandra table schema information

### Fault Tolerance

- High write availability ensures sensor data is accepted even when cluster nodes fail
- Graceful error handling for unavailable nodes with appropriate error responses
- Prepared statements for optimized query performance and security

## Data Schema

### Cassandra Keyspace and Types

```cql
KEYSPACE weather
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

TYPE station_record (
  tmin INT,
  tmax INT
);
```

### Stations Table

```cql
TABLE stations (
  id text,           -- Station ID (partition key)
  date date,         -- Observation date (clustering key, ascending)
  name text static,  -- Station name (static per partition)
  record station_record,  -- Temperature record (tmin, tmax)
  PRIMARY KEY (id, date)
);
```

## Setup and Installation

### Prerequisites

- Docker and Docker Compose
- Environment variable `PROJECT` set to desired prefix (e.g., `p6`)

### Build and Deploy

```bash
# Set project environment variable
export PROJECT=p6

# Build Docker image
docker build . -t p6

# Start 3-node Cassandra cluster
docker compose up -d

# Wait for cluster initialization (1-2 minutes)
docker exec p6-db-1 nodetool status
```

### Generate gRPC Stubs

```bash
docker exec -w /src p6-db-1 sh -c "python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. station.proto"
```

### Start Server

```bash
docker exec -it -w /src p6-db-1 python3 server.py
```

## Usage

### Client Operations

#### 1. View Schema

```bash
docker exec -w /src p6-db-1 python3 ClientStationSchema.py
```

#### 2. Lookup Station Name

```bash
docker exec -w /src p6-db-1 python3 ClientStationName.py <STATION_ID>
```

Example:

```bash
docker exec -w /src p6-db-1 python3 ClientStationName.py US1WIMR0003
# Output: AMBERG 1.3 SW
```

#### 3. Record Temperature Data

```bash
docker exec -w /src p6-db-1 python3 ClientRecordTemps.py
```

#### 4. Query Maximum Temperature

```bash
docker exec -w /src p6-db-1 python3 ClientStationMax.py <STATION_ID>
```

Example:

```bash
docker exec -w /src p6-db-1 python3 ClientStationMax.py USR0000WDDG
# Output: 344 (34.4°C in tenths of degrees)
```

## Implementation Details

### gRPC Services

The system implements four RPC methods:

1. **StationSchema**: Returns the Cassandra table schema
2. **StationName**: Retrieves station name for a given station ID
3. **RecordTemps**: Inserts temperature readings with high write availability
4. **StationMax**: Queries maximum temperature with strong read consistency

### Data Processing

- Spark processes ghcnd-stations.txt using SUBSTRING operations to extract station metadata
- Filters data for Wisconsin state stations
- Batch inserts station information into Cassandra

### Consistency Guarantees

- Write operations use CL=ONE for maximum availability during node failures
- Read operations use CL=THREE to prevent stale data reads
- Ensures R + W > RF (3 + 1 > 3) for linearizable consistency

## Error Handling

The system handles two primary fault scenarios:

- **cassandra.Unavailable**: Insufficient replicas available for requested consistency level
- **cassandra.cluster.NoHostAvailable**: No Cassandra nodes reachable

Both scenarios return error response with "unavailable" status to clients.

## Project Structure

```
.
├── Dockerfile                  # Container configuration
├── docker-compose.yml          # Multi-container orchestration
├── cassandra.sh               # Cassandra initialization script
├── requirements.txt           # Root-level Python dependencies
├── LICENSE                    # Project license
└── src/
    ├── server.py              # gRPC server implementation
    ├── station.proto          # gRPC service definitions
    ├── ClientStationSchema.py # Schema inspection client
    ├── ClientStationName.py   # Station lookup client
    ├── ClientRecordTemps.py   # Temperature recording client
    ├── ClientStationMax.py    # Max temperature query client
    ├── ghcnd-stations.txt     # NOAA station metadata
    ├── weather.parquet        # Weather observation data
    └── requirements.txt       # Additional Python dependencies
```

## Testing Fault Tolerance

To simulate node failure:

```bash
# Stop one Cassandra node
docker stop p6-db-2

# Verify cluster status
docker exec p6-db-1 nodetool status

# Test write operations (should succeed with CL=ONE)
docker exec -w /src p6-db-1 python3 ClientRecordTemps.py

# Test read operations (will return "unavailable" error with CL=THREE)
docker exec -w /src p6-db-1 python3 ClientStationMax.py USR0000WDDG
```

## Academic Context

This project was developed as part of CS544: Big Data Systems coursework, focusing on distributed systems design, fault tolerance, and consistency tradeoffs in NoSQL databases.
