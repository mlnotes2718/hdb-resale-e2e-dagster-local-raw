# HDB Resale End-to-End Data Pipeline with Dagster

This repository implements an end-to-end data engineering pipeline for Singapore HDB resale flat transactions. The workflow combines:

- Dagster for orchestration and asset management
- Meltano for extraction and loading
- dbt for staging and transformation
- Postgres / Supabase as the source data store
- BigQuery as an analytical destination

## Key Components

- `dagster_dbt_integration_hdb_resale/`: Dagster project code defining assets, schedules, and project settings.
- `dbt_hdb_resale/`: dbt models and configuration for staging and transforming HDB resale data.
- `meltano_hdb_resale/`: Meltano project files for extractor/loader configuration and pipeline orchestration.
- `data/`: source dataset and helper scripts for splitting and preparing sample CSV files.

## Getting Started

To set up the project locally:

1. Create the Conda environment from `hdb-dagster-environment.yml`.
2. If you want a fresh rebuild, remove the existing `dagster_dbt_integration_hdb_resale/`, `dbt_hdb_resale/`, and `meltano_hdb_resale/` folders.
3. Follow the setup instructions in `docs/hdb_resale_e2e_dagster_SETUP.md`.

This README is a high-level summary; the `docs/` files contain the detailed configuration and data setup steps.

## How to Prepare HDB Resale Data

Use `docs/hdb_resale_e2e_dagster_SETUP.md` as the main guide for preparing the HDB resale source data. That document links to `docs/supabase_db_setup_hdb.md` and includes the recommended Supabase/Postgres connection details, table names, and incremental replication configuration.

### Demonstrating Meltano Incremental Updates

To simulate incremental data ingestion, run the Python helper script at `data/split_data.py`. It reads the raw CSV file `data/ResaleflatpricesbasedonregistrationdatefromJan2017onwards.csv`, adds placeholder `created_at` and `updated_at` columns, and writes several CSV chunks into `data/split/`.

Example:

```bash
cd data
python split_data.py
```

Generated sample files:

- `data/split/ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part1_200k.csv`
- `data/split/ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part2_5k.csv`
- `data/split/ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part3_5k.csv`
- `data/split/ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part4_5k.csv`
- `data/split/ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part5_5k.csv`
- `data/split/ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part6_5k.csv`
- `data/split/ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part7_5k.csv`
- `data/split/ResaleflatpricesbasedonregistrationdatefromJan2017onwards_part8_remaining.csv`

Load the first file to initialize the dataset, then load later files one by one to demonstrate incremental replication with the `updated_at` key.

