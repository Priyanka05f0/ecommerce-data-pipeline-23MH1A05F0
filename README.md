# E-Commerce Data Pipeline – End-to-End ETL Project

## Project Overview
This project implements a complete end-to-end ETL (Extract, Transform, Load) data pipeline for an e-commerce platform.  
The pipeline generates synthetic data, performs ingestion, validation, transformation, warehouse loading, analytics generation, and visualization readiness.

The system is fully containerized using Docker and validated using automated tests.

---

## Project Architecture

The pipeline follows a layered architecture to ensure scalability, maintainability, and analytical performance.

### Data Flow
```
Raw CSV Data
↓
Staging Schema
↓
Production Schema
↓
Data Warehouse (Star Schema)
↓
Analytics Tables
↓
BI Dashboard (Power BI / Tableau)
```

---

## Technology Stack

| Layer | Technology |
|-----|-----------|
| Data Generation | Python (Faker) |
| Database | PostgreSQL 14 |
| ETL Processing | Python (Pandas, psycopg2) |
| Orchestration | Python Pipeline Orchestrator |
| Data Validation | Custom Quality Checks |
| Containerization | Docker, Docker Compose |
| Testing | Pytest |
| BI Tools | Power BI Desktop / Tableau Public |

---

## Project Structure
```
ecommerce-data-pipeline/
│
├── data/
│ ├── raw/
│ ├── processed/
│
├── scripts/
│ ├── data_generation/
│ ├── ingestion/
│ ├── quality_checks/
│ ├── transformation/
│ ├── monitoring/
│ ├── pipeline_orchestrator.py
│ ├── scheduler.py
│
├── sql/
│ ├── ddl/
│
├── tests/
│
├── docs/
│ ├── architecture.md
│ ├── dashboard_guide.md
│
├── docker/
│ ├── Dockerfile
│ ├── docker-compose.yml
│
├── logs/
├── README.md
└── pytest.ini
```


---

## Setup Instructions

### Prerequisites
- Docker Desktop
- Git
- Python 3.10+ (optional for local execution)

### Setup Steps
```bash
git clone <https://github.com/Priyanka05f0/ecommerce-data-pipeline-23MH1A05F0>
cd ecommerce-data-pipeline
docker-compose -f docker/docker-compose.yml up --build
```
---

# Running the Pipeline
## Full Pipeline Execution
```bash
docker-compose -f docker/docker-compose.yml run --rm pipeline \
python scripts/pipeline_orchestrator.py
```
# Run Individual Steps
```bash
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/quality_checks/validate_data.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py
```

---

# Running Tests
```bash
docker-compose -f docker/docker-compose.yml run --rm pipeline pytest -v
```
---

# Dashboard Access

1. Power BI File: dashboards/powerbi/ecommerce_analytics.pbix

2. Tableau Public URL: (Optional – if published)

---

# Database Schemas
## Staging Schema

staging.customers

staging.products

staging.transactions

staging.transaction_items

## Production Schema

production.customers

production.products

production.transactions

production.transaction_items

## Warehouse Schema

warehouse.dim_customers

warehouse.dim_products

warehouse.dim_date

warehouse.dim_payment_method

warehouse.fact_sales

warehouse.agg_daily_sales

warehouse.agg_product_performance

warehouse.agg_customer_metrics

---

# Key Insights from Analytics

Identification of top-performing product categories

Monthly revenue trends

Customer segmentation insights

Geographic sales distribution

Payment method preferences

# Challenges & Solutions

Data consistency issues: Solved using validation rules

Schema integrity: Enforced via foreign keys

Performance: Optimized using star schema and aggregations

# Future Enhancements

Real-time streaming with Kafka

Cloud deployment (AWS/GCP/Azure)

Advanced ML-based predictions

Real-time alerting and monitoring

---

# Contact

Name: Lakshmi Priyanka Bethampudi
Roll Number : 23MH1A05F0
Email : 23mh1a05f0@acoe.edu.in
