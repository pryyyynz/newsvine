# Newsvine Phase 5 dbt project

This dbt project is designed for local-first development and cloud migration.

## Local target

Use `local_postgres` first so models run against the local Postgres data populated by the phase 5 interaction and article consumers.

1. Install adapters:
   - `pip install dbt-core dbt-postgres dbt-bigquery`
2. Copy `profiles.example.yml` to your dbt profiles folder as `profiles.yml`.
3. Run:
   - `dbt debug --project-dir analytics/dbt_newsvine --target local_postgres`
   - `dbt run --project-dir analytics/dbt_newsvine --target local_postgres`
   - `dbt test --project-dir analytics/dbt_newsvine --target local_postgres`

## Migration target

Switch target to `prod_bigquery` once raw ingestion is mirrored in BigQuery.

- Keep model SQL unchanged where possible.
- Macros in `macros/` abstract adapter-specific functions (JSON extraction and time math).
- Keep source table names stable between Postgres and BigQuery to reduce migration effort.
