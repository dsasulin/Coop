# Banking ETL - Docker Setup

## Overview

This directory contains Docker configuration for running the Banking ETL pipeline locally.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose Setup                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│  │  PostgreSQL  │◄───┤     Hive     │◄───┤    Spark     │ │
│  │              │    │  Metastore   │    │   Master     │ │
│  └──────────────┘    └──────────────┘    └──────────────┘ │
│         │                                        ▲          │
│         │                                        │          │
│         ▼                                        │          │
│  ┌──────────────┐                       ┌──────────────┐  │
│  │   Airflow    │                       │    Spark     │  │
│  │  (Web+Sched) │                       │   Workers    │  │
│  └──────────────┘                       └──────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Services

### 1. PostgreSQL
- **Port:** 5432
- **Purpose:** Backend for Airflow and Hive Metastore
- **Credentials:** etl_user / etl_password

### 2. Hive Metastore
- **Port:** 9083
- **Purpose:** Metadata storage for Spark SQL tables
- **Database:** PostgreSQL (metastore)

### 3. Spark Master
- **Web UI:** http://localhost:8080
- **Master Port:** 7077
- **Purpose:** Spark cluster coordinator

### 4. Spark Workers
- **Web UI:** http://localhost:8081
- **Replicas:** 2
- **Resources:** 2 cores, 4GB RAM per worker

### 5. Airflow Webserver
- **Web UI:** http://localhost:8082
- **Credentials:** admin / admin
- **Purpose:** Workflow orchestration UI

### 6. Airflow Scheduler
- **Purpose:** DAG scheduling and execution

## Quick Start

### 1. Prerequisites
```bash
# Install Docker and Docker Compose
docker --version
docker-compose --version

# Navigate to project root
cd /path/to/Coop
```

### 2. Initialize Environment
```bash
# Create .env file
make init-env

# Edit .env with your settings
vim .env
```

### 3. Start Services
```bash
# Build and start all services
make docker-build
make docker-up

# Or use bootstrap for complete setup
make bootstrap
```

### 4. Initialize Airflow
```bash
# Create admin user
make init-airflow
```

### 5. Access Services
- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081
- Airflow UI: http://localhost:8082 (admin/admin)

## Common Commands

### Service Management
```bash
# Start services
make docker-up

# Stop services
make docker-down

# Restart services
make docker-restart

# View logs
make docker-logs

# View Spark logs only
make docker-logs-spark

# View service status
make docker-ps
```

### Development
```bash
# Open PySpark shell
make pyspark

# Submit Spark job
make spark-submit JOB=01_stage_to_bronze_refactored.py

# Open Airflow bash
make airflow-bash
```

### ETL Commands
```bash
# Run Stage to Bronze
make etl-stage-to-bronze

# Run Bronze to Silver
make etl-bronze-to-silver

# Run Silver to Gold
make etl-silver-to-gold

# Run full pipeline
make etl-full
```

### Testing
```bash
# Run tests
make test

# Run tests with coverage
make test-coverage
```

### Cleanup
```bash
# Clean Python cache
make clean

# Stop and remove volumes
make docker-down-volumes

# Complete cleanup
make clean-all
```

## Directory Structure

```
docker/
├── Dockerfile.spark          # Spark container image
├── Dockerfile.airflow        # Airflow container image
├── docker-entrypoint-spark.sh # Spark startup script
├── spark-defaults.conf       # Spark configuration
├── init-db.sql               # PostgreSQL init script
└── README.md                 # This file
```

## Volumes

- `postgres_data` - PostgreSQL data
- `hive_metastore_data` - Hive warehouse
- `spark_logs` - Spark application logs
- `airflow_logs` - Airflow logs

## Networking

All services communicate via `banking_etl_network` bridge network.

## Troubleshooting

### Services won't start
```bash
# Check logs
make docker-logs

# Check specific service
docker-compose logs postgres
docker-compose logs hive-metastore
```

### Airflow database not initialized
```bash
# Initialize Airflow DB
make init-airflow
```

### Spark job fails
```bash
# Check Spark logs
make docker-logs-spark

# Check application logs
docker-compose exec spark-master cat /opt/spark/logs/*.log
```

### Reset everything
```bash
# Complete reset
make docker-down-volumes
make clean-all
make bootstrap
```

## Performance Tuning

### Increase Spark Resources
Edit `docker-compose.yml`:
```yaml
spark-worker:
  environment:
    - SPARK_WORKER_CORES=4      # Increase cores
    - SPARK_WORKER_MEMORY=8g    # Increase memory
```

### Scale Workers
```bash
# Scale to 4 workers
docker-compose up -d --scale spark-worker=4
```

## Security Notes

⚠️ **This setup is for DEVELOPMENT ONLY**

For production:
1. Change default passwords
2. Use secrets management (Docker secrets, Vault)
3. Enable SSL/TLS
4. Configure firewalls
5. Use proper authentication

## Support

For issues:
1. Check logs: `make docker-logs`
2. Review documentation: `/docs`
3. Open GitHub issue

## Next Steps

1. Load sample data: `make generate-data`
2. Run ETL pipeline: `make etl-full`
3. View results in Spark UI
4. Create Airflow DAG
5. Monitor execution
