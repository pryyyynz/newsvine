# Phase 6 Operations Runbook

## Deployment steps

1. Build API and worker images.
2. Deploy API and pipeline workers.
3. Confirm dependencies: Postgres, Redis, Kafka, Elasticsearch.
4. Run smoke checks:
   - `/health`
   - `/ready`
   - `/metrics`
   - `/search?q=...`

## Rollback procedure

1. Roll back API deployment to previous image tag.
2. Keep Kafka and Postgres data intact.
3. Verify `/recommendations` fallback and `/trending/global` are healthy.
4. Re-run smoke checks.

## Common failure modes

- Elasticsearch unavailable:
  - Symptom: search/news endpoints return 503.
  - Action: check cluster health and index mappings.

- Redis unavailable:
  - Symptom: recommendation cache or rate-limit degradation.
  - Action: restore Redis and verify keyspace + refresh jobs.

- Kafka lag high:
  - Symptom: delayed profile/trending updates.
  - Action: scale consumer workers, inspect lag dashboards.

## On-call escalation path

1. Primary on-call engineer.
2. Data platform owner (Kafka/Redis/ES issues).
3. Application owner.

Include incident start time, impact, and mitigation attempts in escalation message.
