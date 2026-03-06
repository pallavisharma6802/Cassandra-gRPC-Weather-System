# Cassandra gRPC Weather System

A distributed weather data ingestion and query system using Apache Cassandra, gRPC, and Apache Spark — designed around real NOAA station data with explicit consistency/availability tradeoffs.

## Tech Stack

| Layer | Tools |
|---|---|
| Database | Apache Cassandra 5.0.0 (3-node cluster) |
| Communication | gRPC + Protocol Buffers |
| ETL | Apache Spark 3.5.5 |
| Infrastructure | Docker, Docker Compose |
| Language | Python 3 |

## Architecture & Design Decisions

**Cluster:** 3-node Cassandra setup with RF=3

**Consistency model (intentional tradeoff):**
- Writes at `CL=ONE` — maximizes availability; sensor data is never dropped during node failures
- Reads at `CL=THREE` — satisfies R + W > RF, guaranteeing no stale reads
- Net effect: strong read consistency with fault-tolerant writes

**Schema:**
- Partition key on `station_id`, clustering key on `date`
- Static columns for station metadata (stored once per partition, not per row)

## What It Does

- Ingests daily min/max temperature readings from NOAA weather stations via gRPC
- Stores and queries station metadata + observations from a Cassandra cluster
- Preprocesses raw `ghcnd-stations.txt` with Spark (substring parsing, state filtering)
- Exposes 4 RPC methods: `StationSchema`, `StationName`, `RecordTemps`, `StationMax`
- Gracefully handles node failures — returns `"unavailable"` on `Unavailable` and `NoHostAvailable` exceptions

## Fault Tolerance Demo
```bash
# Take down a node
docker stop p6-db-2

# Writes still succeed (CL=ONE needs only 1 replica)
docker exec -w /src p6-db-1 python3 ClientRecordTemps.py  # ✓

# Reads fail gracefully (CL=THREE needs all 3)
docker exec -w /src p6-db-1 python3 ClientStationMax.py USR0000WDDG  # → "unavailable"
```

## Quick Start
```bash
export PROJECT=p6
docker build . -t p6
docker compose up -d

# Wait ~2 min for cluster init, then verify
docker exec p6-db-1 nodetool status

# Generate gRPC stubs and start server
docker exec -w /src p6-db-1 sh -c "python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. station.proto"
docker exec -it -w /src p6-db-1 python3 server.py
```

**Requirements:** Docker, Docker Compose

## Project Structure
```
├── docker-compose.yml
├── Dockerfile
├── cassandra.sh             # Cluster init script
└── src/
    ├── server.py            # gRPC server
    ├── station.proto        # Service definitions
    ├── Client*.py           # One client per RPC method
    ├── ghcnd-stations.txt   # NOAA station metadata
    └── weather.parquet      # Observation data
```
