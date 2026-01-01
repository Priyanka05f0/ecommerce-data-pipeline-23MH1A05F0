# E-Commerce Data Pipeline Architecture

## Overview
This document describes the architecture, data flow, and key design decisions of the end-to-end e-commerce analytics data pipeline.

The pipeline is designed to handle data generation, ingestion, validation, transformation, warehousing, analytics, and visualization in a modular and scalable manner.

---

## High-Level Data Flow

```
Synthetic CSV Data
↓
Staging Schema (PostgreSQL)
↓
Production Schema (PostgreSQL)
↓
Data Warehouse (Star Schema)
↓
Analytics Tables
↓
BI Dashboards (Power BI / Tableau)

---

## System Components

### 1. Data Generation Layer
**Purpose:**  
To simulate realistic e-commerce transactional data for pipeline testing and analytics.

**Responsibilities:**
- Generates synthetic datasets using Python Faker
- Ensures consistent schema and referential integrity
- Outputs CSV files for downstream processing

**Generated Files:**
- `customers.csv`
- `products.csv`
- `transactions.csv`
- `transaction_items.csv`

---

### 2. Data Ingestion Layer
**Purpose:**  
To load raw CSV data into the database safely and efficiently.

**Responsibilities:**
- Reads CSV files from the data directory
- Loads data into PostgreSQL staging schema
- Ensures idempotent batch ingestion

**Technology:**
- Python
- psycopg2
- PostgreSQL

**Pattern Used:**  
Batch ingestion with transactional integrity.

---

### 3. Data Storage Layer

#### a) Staging Schema
**Purpose:**
- Temporary landing zone for raw data
- Enables data inspection and validation before transformation

**Characteristics:**
- Minimal constraints
- Raw structure preserved
- Short-lived storage

---

#### b) Production Schema
**Purpose:**
- Stores clean, validated, and consistent data
- Acts as the system of record

**Characteristics:**
- Normalized (3NF) schema
- Primary and foreign key constraints
- Business rule enforcement

---

#### c) Warehouse Schema
**Purpose:**
- Optimized for analytical queries and reporting

**Characteristics:**
- Star schema design
- Fact and dimension tables
- Supports fast aggregations and slicing

**Key Tables:**
- Dimensions: customers, products, date, payment_method
- Fact: sales

---

### 4. Data Processing Layer
**Purpose:**  
To validate, clean, and transform data for analytical readiness.

**Responsibilities:**
- Data quality checks (nulls, duplicates, invalid values)
- Transformation from staging to production
- Aggregation for warehouse analytics

---

### 5. Data Serving Layer
**Purpose:**  
To provide fast and reliable access to analytical data.

**Responsibilities:**
- Pre-computed aggregation tables
- Analytical SQL queries
- Supports BI tools and reporting use cases

---

### 6. Visualization Layer
**Purpose:**  
To enable business users to explore and analyze insights visually.

**Responsibilities:**
- Power BI / Tableau dashboards
- Interactive filters and drill-downs
- KPI monitoring and trend analysis

**Key Metrics:**
- Revenue trends
- Product performance
- Customer growth
- Payment method distribution

---

### 7. Orchestration & Monitoring Layer
**Purpose:**  
To automate and monitor pipeline execution.

**Responsibilities:**
- Sequential execution of pipeline steps
- Retry logic for transient failures
- Scheduling for periodic runs
- Monitoring pipeline health and freshness

---

## Design Decisions & Rationale

- **PostgreSQL**  
  Chosen for strong relational integrity, ACID compliance, and SQL analytics support.

- **Staging → Production → Warehouse Separation**  
  Improves data quality, traceability, and maintainability.

- **Star Schema**  
  Optimized for analytical queries and BI dashboards.

- **Docker & Docker Compose**  
  Ensures consistent, reproducible environments across systems.

- **Pytest**  
  Enforces correctness and prevents regression issues.

---

## Scalability Considerations
- Modular pipeline components
- Easily extendable to cloud platforms
- Supports future real-time streaming enhancements
