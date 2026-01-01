```md
# E-Commerce Data Pipeline Architecture

## Overview
This document describes the architecture and design decisions of the e-commerce analytics data pipeline.

---

## System Components

### 1. Data Generation Layer
- Generates synthetic e-commerce data using Python Faker
- Outputs CSV files:
  - customers
  - products
  - transactions
  - transaction_items

---

### 2. Data Ingestion Layer
- Loads CSV data into PostgreSQL staging schema
- Technology: Python + psycopg2
- Pattern: Batch ingestion

---

### 3. Data Storage Layer

#### Staging Schema
- Raw data stored as-is
- Minimal validation
- Temporary storage

#### Production Schema
- Cleaned and validated data
- 3NF normalized design
- Foreign key constraints enforced

#### Warehouse Schema
- Star schema design
- Optimized for analytics
- Fact and dimension tables

---

### 4. Data Processing Layer
- Data quality validation
- Cleansing and transformations
- Aggregation tables for performance

---

### 5. Data Serving Layer
- Analytical SQL queries
- Pre-computed aggregates
- BI-tool connectivity

---

### 6. Visualization Layer
- Power BI / Tableau dashboards
- Interactive filters and drill-downs
- Business KPI reporting

---

### 7. Orchestration Layer
- Pipeline orchestrator executes steps sequentially
- Retry logic for failures
- Scheduler enables automated runs
- Monitoring tracks pipeline health

---

## Design Decisions
- PostgreSQL chosen for reliability and relational integrity
- Star schema used for fast analytical queries
- Docker ensures reproducible deployment
- Pytest enforces data correctness
