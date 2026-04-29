# ADR-0003: Recommendation Scoring Weights

## Status
Accepted

## Context
Serving combines content similarity, trending boost, and collaborative score.

## Decision
Default blended score:
- content weight: 0.6
- trending weight: 0.3
- collaborative weight: 0.1

## Consequences
- Balances personalization with freshness.
- Weights remain configurable via environment settings.
