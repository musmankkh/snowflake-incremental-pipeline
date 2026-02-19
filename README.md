# Snowflake Incremental ELT Pipeline

A fully automated, production-grade ELT pipeline that loads sales data incrementally into Snowflake month by month, transforms and validates it using dbt, and runs hands-free via GitHub Actions.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Pipeline Flow](#pipeline-flow)
- [Data Layers](#data-layers)
- [Incremental Strategy](#incremental-strategy)
- [GitHub Actions Automation](#github-actions-automation)
- [Branch Strategy](#branch-strategy)
- [Setup & Installation](#setup--installation)
- [GitHub Secrets Configuration](#github-secrets-configuration)
- [Data Quality Checks](#data-quality-checks)
- [Processed Layer — Derived Columns](#processed-layer--derived-columns)

---

## Overview

This pipeline solves a common data engineering problem: loading a large historical dataset into a data warehouse **incrementally**, without re-uploading data that already exists, and transforming only the **new data** each day.

The pipeline:
- Loads **one month of data per day** into Snowflake
- Skips months that are already loaded (no duplicates)
- Transforms raw data through a **Staging** layer (type casting, date fixing, quality flagging)
- Produces a clean **Processed** layer (deduplication, enrichment, derived metrics)
- Runs **automatically every day** via GitHub Actions — no manual intervention needed

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCE                              │
│               online_sales_dataset.csv                          │
│         (full historical dataset, all months)                   │
└───────────────────────────┬─────────────────────────────────────┘
                            │  Python Script (loaddata.py)
                            │  • Checks which months already exist
                            │  • Uploads ONE new month per run
                            │  • Stops after upload
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE — LANDINGZONE                       │
│                        RAW_SALES                                │
│              Raw data, no transformations                        │
│              InvoiceDate stored as VARCHAR                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │  dbt — stg_sales.sql
                            │  • Cast all data types
                            │  • Fix InvoiceDate VARCHAR → TIMESTAMP
                            │  • Trim & uppercase categoricals
                            │  • Flag bad records (data_quality_flag)
                            │  • Incremental by month
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE — STAGING                           │
│                        STG_SALES                                │
│         Type-safe data with quality flags applied               │
└───────────────────────────┬─────────────────────────────────────┘
                            │  dbt — processed_sales.sql
                            │  • Remove duplicates
                            │  • Keep only VALID records
                            │  • Remove negative qty/price
                            │  • Add derived columns
                            │  • Incremental by month
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE — PROCESSED                         │
│                      PROCESSED_SALES                            │
│         Clean, deduplicated, enriched — ready for analytics     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **Python** | Incremental CSV → Snowflake loader |
| **Snowflake** | Cloud data warehouse |
| **dbt (Snowflake adapter)** | Data transformation & testing |
| **GitHub Actions** | Automated daily scheduling |
| **GitHub Secrets** | Secure credential management |

---

## Project Structure

```
snowflake-incremental-pipeline/
│
├── .github/
│   └── workflows/
│       ├── dev_pipeline.yml        ← DEV: runs daily at 9AM UTC
│       └── main_pipeline.yml       ← PROD: runs daily at 10AM UTC
│
├── python/
│   ├── loaddata.py                 ← Incremental CSV loader
│   └── online_sales_dataset.csv    ← Source dataset
│
├── dbt_project/
│   ├── dbt_project.yml             ← dbt project configuration
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml         ← RAW_SALES source definition
│   │   │   └── stg_sales.sql       ← Staging transformation model
│   │   └── processed/
│   │   |    ├── schema.yml          ← Column tests & documentation
│   │   |    └── processed_sales.sql ← Processed transformation model
│       |__ marts/
|              |__dimensions/
|              |__facts/
|              
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Pipeline Flow

Each day the pipeline executes these steps in order:

```
Step 1 — Python (loaddata.py)
         Check which months are already in Snowflake RAW_SALES
         → If new month found: upload it, then stop
         → If all months loaded: exit gracefully

Step 2 — dbt run (stg_sales)
         Read new month from RAW_SALES
         Cast types, fix dates, flag quality issues
         Write to STAGING.STG_SALES

Step 3 — dbt run (processed_sales)
         Read from STG_SALES
         Deduplicate, filter valid records, enrich
         Write to PROCESSED.PROCESSED_SALES

Step 4 — dbt test
         Run automated data quality tests
         Fail pipeline if critical tests fail
```

---

## Data Layers

### Layer 1 — Raw (LANDINGZONE.RAW_SALES)
The landing zone for data exactly as it comes from the CSV. No transformations applied. `InvoiceDate` is stored as `VARCHAR` to avoid parsing issues at load time.

### Layer 2 — Staging (STAGING.STG_SALES)
Applies all type casting, date parsing, and data quality flagging. Every record is kept — including bad ones — but each gets a `data_quality_flag` label so nothing is silently lost.

| Column | Type | Notes |
|---|---|---|
| invoice_no | VARCHAR | Unique identifier |
| invoice_date | TIMESTAMP | Parsed from VARCHAR using `TRY_TO_TIMESTAMP` |
| quantity | INT | Cast with `TRY_CAST` |
| unit_price | FLOAT | Cast with `TRY_CAST` |
| discount | FLOAT | Cast with `TRY_CAST` |
| shipping_cost | FLOAT | Cast with `TRY_CAST` |
| country | VARCHAR | Trimmed & uppercased |
| payment_method | VARCHAR | Trimmed & uppercased |
| data_quality_flag | VARCHAR | VALID or reason for rejection |
| stg_loaded_at | TIMESTAMP | Audit column |

### Layer 3 — Processed (PROCESSED.PROCESSED_SALES)
Clean, analytics-ready data. Only `VALID` records from staging pass through. Duplicates are removed and derived business metrics are added.

---

## Incremental Strategy

Both dbt models use **month-based incremental loading**:

```sql
{% if is_incremental() %}
    WHERE (invoice_year, invoice_month) NOT IN (
        SELECT DISTINCT invoice_year, invoice_month
        FROM {{ this }}
    )
{% endif %}
```

This means:

```
Day 1  →  January loaded to RAW   →  Staging & Processed transform January   →  STOP
Day 2  →  February loaded to RAW  →  January skipped, February transformed   →  STOP
Day 3  →  March loaded to RAW     →  Jan & Feb skipped, March transformed    →  STOP
```

No month is ever processed twice.

---

## GitHub Actions Automation

The pipeline runs fully automatically — no manual steps required after initial setup.

### DEV Workflow (`dev_pipeline.yml`)
- **Trigger:** Daily at 9:00 AM UTC, or on every push to `dev` branch
- **Environment:** dev
- **Purpose:** Test pipeline changes safely before promoting to production

### MAIN Workflow (`main_pipeline.yml`)
- **Trigger:** Daily at 10:00 AM UTC, or when a PR is merged into `main`
- **Environment:** production
- **Purpose:** Production data pipeline

Each workflow runs these steps:
1. Checkout repository
2. Setup Python 3.11
3. Install dependencies from `requirements.txt`
4. Run `loaddata.py` — load new month to Snowflake
5. Generate `profiles.yml` from GitHub Secrets at runtime
6. `dbt debug` — verify Snowflake connection
7. `dbt run` — execute staging and processed models
8. `dbt test` — validate data quality

---

## Branch Strategy

| Branch | Environment | Schedule | Trigger |
|---|---|---|---|
| `dev` | Development | 9:00 AM UTC daily | Push to dev / Manual |
| `main` | Production | 10:00 AM UTC daily | PR merge / Manual |

### Workflow
```
feature branch  →  Pull Request  →  dev branch  →  Pull Request  →  main branch
   (develop)          (review)       (test run)       (approve)      (production)
```

---

## Setup & Installation

### Prerequisites
- Python 3.11+
- Snowflake account
- GitHub account

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/snowflake-incremental-pipeline.git
cd snowflake-incremental-pipeline
```

### 2. Create branches
```bash
git checkout -b dev
git push origin dev
git checkout main
```

### 3. Install dependencies locally (optional)
```bash
pip install -r requirements.txt
```

### 4. Add GitHub Secrets
Go to: `GitHub Repo → Settings → Secrets and variables → Actions → New repository secret`

| Secret Name | Description |
|---|---|
| `SNOWFLAKE_USER` | Your Snowflake username |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password |
| `SNOWFLAKE_ACCOUNT` | Account identifier (e.g. `abc123.us-east-1`) |
| `SNOWFLAKE_WAREHOUSE` | Warehouse name |
| `SNOWFLAKE_DATABASE` | Database name |
| `SNOWFLAKE_SCHEMA` | Schema name |

### 5. Push your code
```bash
git add .
git commit -m "Initial pipeline setup"
git push origin dev
```

GitHub Actions will automatically trigger and run the pipeline.

---

## GitHub Secrets Configuration

Credentials are **never stored in the repository**. The `profiles.yml` for dbt is generated dynamically at runtime inside the GitHub Actions workflow using secrets:

```yaml
- name: Configure dbt profiles.yml
  run: |
    mkdir -p ~/.dbt
    cat > ~/.dbt/profiles.yml << EOF
    sales_pipeline:
      target: dev
      outputs:
        dev:
          type: snowflake
          account: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          user: ${{ secrets.SNOWFLAKE_USER }}
          password: ${{ secrets.SNOWFLAKE_PASSWORD }}
    EOF
```

The file is created at runtime, used, then discarded. It never touches the repository.

---

## Data Quality Checks

### Staging Flags

| Flag | Condition |
|---|---|
| `VALID` | Record passed all checks |
| `INVALID_DATE` | `InvoiceDate` could not be parsed |
| `INVALID_QUANTITY` | `Quantity` is NULL |
| `INVALID_PRICE` | `UnitPrice` is NULL |
| `NEGATIVE_SHIPPING` | `ShippingCost` is less than 0 |
| `INVALID_DISCOUNT` | `Discount` is not between 0 and 1 |
| `NULL_INVOICE` | `InvoiceNo` is NULL |

Only records flagged as `VALID` are passed to the Processed layer.

### dbt Tests
Automated tests run after every `dbt run`:
- `invoice_no` — not null, unique (Processed layer)
- `invoice_date` — not null
- `quantity` — not null
- `unit_price` — not null

---

## Processed Layer — Derived Columns

The processed model adds the following business metrics:

| Column | Formula |
|---|---|
| `gross_amount` | `quantity × unit_price` |
| `net_amount` | `quantity × unit_price × (1 - discount)` |
| `total_amount` | `net_amount + shipping_cost` |
| `is_returned` | `TRUE` if `return_status = RETURNED` |
| `is_guest_customer` | `TRUE` if `customer_id` is NULL |