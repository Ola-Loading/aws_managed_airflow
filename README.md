# AWS Managed Airflow Demo

A batch data pipeline built on Amazon MWAA (Managed Workflows for Apache Airflow), simulating
a music-streaming analytics workload: raw CSVs land in S3, get validated, and are aggregated
into KPI reporting tables in Amazon Redshift.

## Structure

- `Airflow-dag/dag-song-kpi-calculations.py` — the DAG (`data_validation_and_kpi_computation`,
  daily schedule). Bootstraps the Redshift schema, validates incoming `songs`, `users`, and
  `streams` files in S3, computes genre-level and hourly KPIs, upserts them into Redshift, then
  archives the processed stream files.
- `Redshift/redshift-tables.sql` — reference DDL for the target database/schema/tables (the DAG
  also creates these idempotently at runtime via its bootstrap task).
- `data/` — sample CSVs (songs, users, streams) for local testing.
- `local_dev/` — notebooks used to prototype the KPI logic before porting it into the DAG.
- `requirements.txt` — Python dependencies for the MWAA environment.

## Flow

```
bootstrap_redshift_schema
        │
        ▼
validate_datasets ──▶ check_validation ──▶ end_dag (on failure)
                              │
                              ▼ (on success)
                  calculate_genre_level_kpis
                              │
                              ▼
                   calculate_hourly_kpis
                              │
                              ▼
                   move_processed_files
```

## Requirements

- An MWAA environment with the `apache-airflow-providers-postgres` provider available.
- An Airflow connection named `redshift_default` pointing at the target Redshift database.
- An S3 bucket (`music-sofar-sessions`) containing `spotify_data/songs.csv`,
  `spotify_data/users.csv`, and `spotify_data/streams/*.csv`.
