# Web App (Usage API demo)

This is a minimal static frontend for the Usage API. It only demonstrates how to call the API and present results.

How to run

1. Ensure the Usage API is running on `http://localhost:5000` (this is the default when running via `docker compose` or running `cdr_usage_api/main.py`).
2. Start the proxy (this avoids CORS issues when calling the API from the browser):

```bash
python web_app/proxy.py
# open http://localhost:8000 in your browser
```

3. Use the form to query daily usage or summary for an MSISDN. Default basic auth credentials are `admin:admin123`.

Notes
- The proxy forwards `/api/*` and `/health` requests to `http://localhost:5000` by default. To point to a different backend, set `USAGE_API_BACKEND` env var.
- This frontend is intentionally minimal for demo purposes. If you want a production-ready UI, I can add a small React app and integrate it into `docker-compose`.
