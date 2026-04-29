# Phase 6 Demo Script

## Goal

Demonstrate that user interactions and search update recommendation behavior in near real-time.

## Flow

1. Register user.
2. Login and capture bearer token.
3. Read an article (`GET /news/{id}` with Authorization header).
4. Like an article (`POST /events`, `event_type=like`).
5. Search for a topic (`GET /search?q=...`).
6. Wait up to 30 seconds for pipeline propagation.
7. Call `GET /recommendations` and confirm ranking has changed.

## Verification checkpoints

- `GET /search` returns ranked items with `relevance_score`.
- `user:<id>:vector` in Redis reflects search topic.
- `/recommendations` returns topic-relevant ordering after interaction propagation.

## Fallback scenario

1. Stop Redis service.
2. Call `/recommendations`.
3. Verify cached/trending fallback response returns cleanly and API does not crash.
