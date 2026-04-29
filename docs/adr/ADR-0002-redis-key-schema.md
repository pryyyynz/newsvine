# ADR-0002: Redis Key Schema

## Status
Accepted

## Context
Recommendation serving requires low-latency lookups for user profile vectors, ALS boosts, and article embeddings.

## Decision
Use stable key patterns:
- `user:{id}:vector`
- `user:{id}:embedding`
- `user:{id}:als`
- `article:{id}:embedding`
- `article:{id}:meta`
- `reco:category:{category}:recent`
- `trending:global`
- `trending:country:{country}`

## Consequences
- Predictable cache invalidation.
- Easier migration between local and cloud environments.
