# Redis keyspace conventions

- user:{id}:vector
- trending:global
- trending:country:{cc}
- article:{id}:embedding

Notes:
- Use TTL on transient keys.
- Prefer hashes for multi-field values.
- Keep key prefixes stable for easier invalidation patterns.
