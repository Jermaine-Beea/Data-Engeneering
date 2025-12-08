# Demo Commands

Quick copy/paste commands for the presentation. Run from the project root.

Start the full stack (recommended):

```bash
./reset-env.sh
# or
docker compose up -d
```

Start only core services:

```bash
docker compose up -d postgres redpanda-0 redpanda-1 redpanda-2 sftp
docker compose up -d cdr prepared-layers cdr-usage-api
```

Run unit tests:

```bash
pytest -q
```

Show logs for a service:

```bash
docker compose logs -f prepared-layers
docker compose logs -f cdr
docker compose logs -f cdr-usage-api
```

Inspect Postgres (host mapping per repo docs):

```bash
psql -h localhost -p 15432 -U postgres -d wtc_analytics -c "SELECT * FROM prepared_layers.cdr_usage_summary_15min LIMIT 5;"
```

Redpanda console and topics:

```bash
# Redpanda Console: http://localhost:18084
docker exec -it redpanda-0 rpk topic list
docker exec -it redpanda-0 rpk topic consume cdr_data --num 10
```

Usage API examples:

```bash
# Health (no auth)
curl http://localhost:5000/health

# Daily usage (basic auth)
curl -u admin:admin123 "http://localhost:5000/api/usage/7643008066273?start_date=2024-01-01&end_date=2024-01-31"

# Aggregate summary
curl -u admin:admin123 http://localhost:5000/api/usage/7643008066273/summary
```

Run the demo frontend (serves the static UI and proxies /api to avoid CORS):

```bash
python web_app/proxy.py
# Open http://localhost:8000 in your browser
```

If the proxy is not suitable, you can directly run the static file (may hit CORS):

```bash
python -m http.server 8000
# then open http://localhost:8000
```
