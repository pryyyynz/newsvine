# ADR-0004: ALS Hyperparameters

## Status
Accepted

## Context
Need stable baseline for collaborative filtering model training.

## Decision
Use ALS defaults for phase rollout:
- `rank=50`
- `maxIter=20`
- `regParam=0.1`
- evaluate with NDCG@20 on validation split

## Consequences
- Predictable baseline during rollout.
- Future tuning should be benchmarked against production NDCG@20.
