# Docker Deployment Guide – E-Commerce Data Pipeline

## 1. Prerequisites

- Docker >= 20.x
- Docker Compose >= v2
- Minimum system requirements:
  - RAM: 4 GB
  - Disk Space: 5 GB

---

## 2. Quick Start Guide

### Step 1: Build Docker images
```bash
docker compose -f docker/docker-compose.yml build
```
### Step 2: Start services
```bash
docker compose -f docker/docker-compose.yml up
```
---

## 3. Verify Services
### Check running containers
```bash
docker ps
```
You should see:

- ecommerce-postgres
- ecommerce-pipeline

### Check PostgreSQL health
```bash
docker inspect --format='{{.State.Health.Status}}' ecommerce-postgres
```
### Expected output:
```nginx
healthy
```
---

## 4. Running the Pipeline in Containers

The pipeline container automatically runs the full ETL process:
- Data generation
- Ingestion
- Transformation
- Warehouse loading
- Analytics generation
#### To run manually:
```bash
docker compose -f docker/docker-compose.yml run pipeline
```
---

## 5. Accessing the Database
```bash
docker exec -it ecommerce-postgres psql -U admin -d ecommerce_db
```
---

## 6. Viewing Logs
```bash
docker logs ecommerce-pipeline
```
---

## 7. Stopping Services
```bash
docker compose -f docker/docker-compose.yml down
```
---

## 8. Cleanup (Remove Volumes & Data)
```bash
docker compose -f docker/docker-compose.yml down -v
```
---

## 9. Troubleshooting

### Port already in use
- Stop local PostgreSQL or change port mapping.

### Database not ready
- Ensure postgres container health is healthy.

### Pipeline cannot connect to DB
- Ensure DB_HOST=postgres (service name, not localhost).

### Data persistence issues
- Verify volume postgres_data exists.

---
