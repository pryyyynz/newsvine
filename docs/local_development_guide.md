# Local Development Guide (Phase 6)

## Target outcome

From clean checkout to running API with dependencies in under 10 minutes.

## Quick start

1. Copy environment template.
2. Start local stack:

```powershell
docker compose -f docker-compose.local.yml up -d
```

3. Verify health:

```powershell
Invoke-WebRequest -Method Get -Uri http://localhost:8000/health
Invoke-WebRequest -Method Get -Uri http://localhost:8000/ready
Invoke-WebRequest -Method Get -Uri http://localhost:8000/metrics
```

4. Run tests:

```powershell
c:/Users/pryyy/Projects/newsvine/.venv/Scripts/python.exe -m pytest -q
```

## Phase 6 smoke endpoints

- `GET /search?q=technology`
- `GET /users/me` (with bearer token)
- `GET /users/me/history`
- `GET /users/me/bookmarks`

## Load test entrypoint

```powershell
k6 run loadtests/phase6_full.js
```
