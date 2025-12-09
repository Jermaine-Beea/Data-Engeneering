# Data Engineering Project Demo — Speaker & Command Handout

---

## Jermaine — Lead Presenter (Coordinator)

**Role:** Boot environment, present architecture, run API/web demo, guide handoffs.

**Script:**
- "Hello — we built an end-to-end data engineering pipeline: generators, streaming, transformations, a high-velocity store, and a Usage API."
- "I’ll boot the stack, show an example API query and the web UI, then hand off to teammates for focused demos."
- "We’ll demonstrate data generation → Redpanda → prepared layers/HVS → API."
- (Before API call) "Now I’ll request usage for a sample MSISDN to show real-time results."
- (Wrap) "To finish, we’ll discuss architecture, key code choices, and planned enhancements."

**Commands:**
```bash
./reset-env.sh
# OR
# docker compose up -d

docker compose ps

curl -u user:password "http://localhost:18089/data_usage?msisdn=2712345678&start_time=20240101000000&end_time=20240101235959"
# Open web app in browser: http://localhost:18080
```

---

## Data Generator — CDR/CRM/Forex

**Role:** Start/describe generators, show produced files & topic messages.

**Script:**
- "I generate synthetic CDR, CRM and Forex data; CDR files are uploaded to SFTP and messages published to Redpanda topics (`cdr_data`, `cdr_voice`, `tick-data`)."
- "I'll show log lines, sample CSV, and Redpanda topic sample."

**Commands:**
```bash
docker compose up -d cdr
# OR
docker compose up cdr

docker compose logs -f cdr

sftp -P 10022 cdr_data@localhost
ls

docker exec -it redpanda-0 rpk topic list
docker exec -it redpanda-0 rpk topic consume cdr_data --num 5

sed -n '1,10p' cdr/cdr_data_20240101_120000.csv # Replace filename as needed
```

---

## Stream Processing / HVS

**Role:** Show consumer reading Redpanda, performing real-time aggregations, writing to HVS.

**Script:**
- "We consume CDR and forex topics, compute near-real-time aggregations (per-msisdn summaries) and write to the high-velocity store for <60s reads."
- "I'll show logs inserting records into HVS and a quick verification query."

**Commands:**
```bash
docker compose up -d hvs
docker compose logs -f hvs
# If HVS is ScyllaDB:
docker exec -it scylla-node cqlsh -e "SELECT msisdn, session_start, session_end FROM hvs.keyspace_usage LIMIT 5;"
# If HVS is Postgres:
docker exec -it postgres psql -U postgres -d wtc_analytics -c "SELECT msisdn, datetime, total_cost_wak FROM prepared_layers.cdr_usage_summary_1hr ORDER BY datetime DESC LIMIT 5;"
```

---

## Prepared Layers — Transformation Service

**Role:** Show prepared layers service producing 15m/30m/1h CDR summaries, tower sessions, forex OHLC + indicators, CRM flattening.

**Script:**
- "Prepared Layers runs every 5 minutes, reads raw tables and writes analyst-ready tables in `prepared_layers`. It replaces dbt/Airflow for this demo."
- "I'll show the service logs and a sample query returning 1-hour summaries."

**Commands:**
```bash
docker compose up -d prepared-layers
docker compose logs -f prepared-layers
docker exec -it postgres psql -U postgres -d wtc_analytics -c "SELECT datetime, msisdn, call_cost_zar, data_cost_zar FROM prepared_layers.cdr_usage_summary_1hr LIMIT 5;"
```

---

## API & Web UI — Usage API & Frontend

**Role:** Show the Usage API returning JSON and the web UI consuming it.

**Script:**
- "Our Usage API serves daily summaries (basic auth). We’ll call it live and then show the web UI that queries the same endpoint."
- "This demonstrates how an external client would integrate with the system."

**Commands:**
```bash
docker compose up -d cdr_usage_api web_app
docker compose logs -f cdr_usage_api
curl -u user:password "http://localhost:18089/data_usage?msisdn=2712345678&start_time=20240101000000&end_time=20240101235959" | jq
# Open web app in browser: http://localhost:18080
```

---

## Final Wrap & Troubleshooting

- If Docker services fail to start: "Ensure Docker has >=8GB memory; raise to 12GB if needed."
- If Redpanda not ready: `docker compose ps` and check `redpanda-*` health. Wait or restart.
- If SFTP refused (dev): Check port 10022 and `sftp` container running: `docker compose up -d sftp`.

---

**Demo Run Checklist (Jermaine can use):**
```bash
./reset-env.sh
docker compose ps
curl -u user:password "http://localhost:18089/data_usage?msisdn=2712345678&start_time=20240101000000&end_time=20240101235959" | jq
docker compose up -d cdr && docker compose logs -f cdr --tail 50
docker compose up -d hvs && docker compose logs -f hvs --tail 50
docker compose up -d prepared-layers && docker compose logs -f prepared-layers --tail 50
docker compose up -d cdr_usage_api web_app && docker compose logs -f cdr_usage_api --tail 50
docker compose ps
```
