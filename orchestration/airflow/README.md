# Airflow Phase 5 orchestration

This directory contains the local-first DAG for phase 5 batch runs.

## Deployment intent

- Local validation: run DAG tasks against Postgres + Redis + MLflow in docker-compose.
- Cloud migration: point the same DAG to BigQuery, GCS, and cluster images by changing Airflow Variables and image tags.

## Required Airflow Variables

- `DBT_IMAGE`
- `SPARK_BATCH_IMAGE`
- `API_WORKER_IMAGE`
- `DBT_TARGET` (example: `local_postgres` now, `prod_bigquery` later)
- `BATCH_NAMESPACE`
- `REDIS_URL`
- `MLFLOW_TRACKING_URI`
- `PHASE5_MLFLOW_EXPERIMENT`
- `BATCH_ALERT_EMAILS` (optional, comma-separated)
- `BATCH_SLACK_WEBHOOK` (optional)

## Local smoke sequence

1. Start local stack with ml profile.
2. Trigger `nightly_batch` manually from Airflow UI.
3. Confirm markers exist in Redis:
   - `phase5:last_refresh:user_vectors`
   - `phase5:last_refresh:article_embeddings`
   - `phase5:last_refresh:als_scores`
4. Confirm latest MLflow run has `ndcg_at_k` metric.

## Migration notes

- Keep source and destination table names aligned across environments.
- Keep Spark job env var names unchanged between local and cloud deployments.
- Move credentials to Secret Manager and map to Kubernetes secrets in Helm values.
