# ADR-0001: Kafka Topic Design

## Status
Accepted

## Context
Needing separate streams for ingestion, interactions, trending updates, and dead-letter handling.

## Decision
Use dedicated topics:
- `news-articles`
- `user-interactions`
- `trending-updates`
- `news-articles-dlq`

## Consequences
- Clear ownership per stream.
- Easier consumer scaling and lag debugging.
- DLQ supports replay and incident recovery.
