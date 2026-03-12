# Bluestone Real Estate – Data Pipeline

End-to-end ETL pipeline that extracts real-estate listing and market data from the [RentCast API](https://www.rentcast.io/), generates synthetic inquiry and transaction records, transforms everything into analysis-ready formats, and uploads the final datasets to Amazon S3.

---

## Table of Contents

- [Overview](#overview)
- [Pipeline Stages](#pipeline-stages)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Output Datasets](#output-datasets)
- [S3 Upload Layout](#s3-upload-layout)

---

## Overview

The pipeline covers **seven US metro areas** — Austin, Houston, Phoenix, Denver, Atlanta, Chicago, and Charlotte — and produces five core datasets (listings, listing history, sale transactions, rental transactions, and inquiries) plus three market-statistics tables, all stored as CSV and Parquet.

---

## Pipeline Stages

| # | Script | Purpose |
|---|--------|---------|
| 1 | `data_extraction.py` | Pulls rental and sale listings from the RentCast Listings API, enriches each listing with market data by zip code, flattens nested JSON fields, and saves the combined result to `data/raw_data/rentcast_properties.csv`. |
| 2 | `market_data.py` | Pulls detailed market statistics (overall, by property type, and by bedroom count) for ~405 zip codes across the target metros and writes three CSVs to `data/market/`. |
| 3 | `data_generator.py` | Reads the enriched listings and generates realistic synthetic inquiries, sale transactions, and rental transactions using property-type profiles and market-aware heuristics. Outputs three CSVs to `data/`. |
| 4 | `data_transformation.py` | Cleans and normalises all raw CSVs — expands embedded JSON columns, explodes listing history, adds contingency flags, enforces nullable dtypes, and writes Parquet + CSV to `data/transformed_data/`. |
| 5 | `data_upload.py` | Uploads raw data to the **bronze** layer and transformed/market data to the **silver** layer of the `bluestone-real-estate-amdari` S3 bucket. |

---

## Project Structure

```
.
├── README.md
├── data_extraction.py          # Stage 1 – API extraction
├── market_data.py              # Stage 2 – Market statistics pull
├── data_generator.py           # Stage 3 – Synthetic data generation
├── data_transformation.py      # Stage 4 – Cleaning & transformation
├── data_upload.py              # Stage 5 – S3 upload
│
└── data/
    ├── raw_data/                       # Bronze – raw source data
    │   ├── rentcast_properties.csv     #   Enriched listings  (stage 1 output)
    │   ├── inquiries.csv               #   Synthetic inquiries (stage 3 output)
    │   ├── sale_transactions.csv       #   Synthetic sales     (stage 3 output)
    │   └── rental_transactions.csv     #   Synthetic rentals   (stage 3 output)
    │
    ├── market/                         # Silver – market stats (stage 2 output)
    │   ├── market_stats.csv
    │   ├── market_stats_by_proptype.csv
    │   ├── market_stats_by_bedrooms.csv
    │   └── bluestone_marketdata.csv
    │
    └── transformed_data/               # Silver – cleaned outputs (stage 4 output)
        ├── csv/
        │   ├── listings.csv
        │   ├── listing_history.csv
        │   ├── sale_transactions.csv
        │   ├── rental_transactions.csv
        │   └── inquiries.csv
        └── parquet/
            ├── listings.parquet
            ├── listing_history.parquet
            ├── sale_transactions.parquet
            ├── rental_transactions.parquet
            └── inquiries.parquet
```

---

## Prerequisites

- **Python ≥ 3.9**
- A [RentCast API key](https://www.rentcast.io/)
- AWS credentials configured (via `aws configure`, environment variables, or an IAM role) with write access to the target S3 bucket
- [`uv`](https://github.com/astral-sh/uv) for dependency management

---

## Setup

1. **Clone the repository**

   ```bash
   git clone <repo-url> && cd <repo-name>
   ```

2. **Install dependencies**

   ```bash
   # With uv
   uv sync
   ```

   Core dependencies: `boto3`, `pandas`, `pyarrow`, `requests`, `faker`, `python-dotenv`

3. **Configure environment variables**

   Create a `.env` file in the project root:

   ```env
   RENT_CAST_API_KEY=<your-rentcast-api-key>
   ```

---

## Usage

Run each stage sequentially from the project root:

```bash
# 1. Extract listings from RentCast API
uv run data_extraction.py

# 2. Pull market statistics by zip code
uv run market_data.py

# 3. Generate synthetic inquiries & transactions
uv run data_generator.py

# 4. Clean, transform, and write final outputs
uv run data_transformation.py

# 5. Upload everything to S3
uv run data_upload.py
```

> **Tip:** Stages 1 and 2 make live API calls and are rate-limited. Stages 3–5 are offline and run in seconds.

---

## Output Datasets

### Final CSV / Parquet (`data/transformed_data/`)

| File | Description |
|------|-------------|
| `listings` | Cleaned property listings with expanded HOA, office, builder, and agent fields. |
| `listing_history` | Long-form table of listing status events (date, price, status changes). |
| `sale_transactions` | Synthetic sale transactions with contingency flags (inspection, financing, appraisal). |
| `rental_transactions` | Synthetic rental transactions with lease terms. |
| `inquiries` | Synthetic buyer/renter inquiries linked to listings. |

### Market Statistics (`data/market/`)

| File | Description |
|------|-------------|
| `market_stats.csv` | Aggregate sale & rental statistics per zip code. |
| `market_stats_by_proptype.csv` | Statistics broken down by property type. |
| `market_stats_by_bedrooms.csv` | Statistics broken down by bedroom count. |

---

## S3 Upload Layout

Files are uploaded to the `bluestone-real-estate-amdari` bucket using a **medallion architecture** (bronze → silver). The **gold** layer is managed downstream by data analysts.

```
s3://bluestone-real-estate-amdari/
├── bronze/                    ← raw, untransformed data
│   ├── listings/
│   ├── inquiries/
│   ├── sale_transactions/
│   └── rental_transactions/
└── silver/                    ← cleaned & transformed data
    ├── listings/
    ├── listing_history/
    ├── sale_transactions/
    ├── rental_transactions/
    ├── inquiries/
    └── market_data/
        ├── market_stats/
        ├── by_property_type/
        └── by_bedrooms/
```
