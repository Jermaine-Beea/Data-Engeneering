import os
from datetime import datetime, timedelta
from functools import wraps
import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify

# Optional Scylla/Cassandra (import lazily / optional)
HAS_CASSANDRA = False
try:
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
    HAS_CASSANDRA = True
except Exception:
    # cassandra-driver not installed; Scylla support will be disabled
    HAS_CASSANDRA = False

app = Flask(__name__)

# Configuration from environment
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": os.getenv("DB_PORT", "5432"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "database": os.getenv("DB_NAME", "wtc_analytics")
}

API_USERNAME = os.getenv("API_USERNAME", "admin")
API_PASSWORD = os.getenv("API_PASSWORD", "admin123")


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)


def get_scylla_session():
    """Create and return a Scylla/Cassandra session connected to `cdr_keyspace`.

    Uses `SCYLLA_HOST` and `SCYLLA_PORT` env vars. Raises on failure.
    """
    if not HAS_CASSANDRA:
        raise RuntimeError("cassandra-driver not available")

    host = os.getenv("SCYLLA_HOST", "scylla")
    port = int(os.getenv("SCYLLA_PORT", "9042"))
    cluster = Cluster(contact_points=[host], port=port)
    session = cluster.connect()
    # Ensure keyspace is available; switch to it
    session.set_keyspace("cdr_keyspace")
    return cluster, session


def require_auth(f):
    """Decorator for basic authentication."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or auth.username != API_USERNAME or auth.password != API_PASSWORD:
            return jsonify({"error": "Authentication required"}), 401, {
                "WWW-Authenticate": 'Basic realm="Login Required"'
            }
        return f(*args, **kwargs)
    return decorated


def build_date_filter(params):
    """Build date filter SQL and parameters."""
    conditions = []
    values = []
    
    if params.get("start_date"):
        conditions.append("DATE(start_time) >= %s")
        values.append(params["start_date"])
    
    if params.get("end_date"):
        conditions.append("DATE(start_time) <= %s")
        values.append(params["end_date"])
    
    return conditions, values


def iterate_dates(start_date, end_date):
    """Yield date objects from start_date to end_date inclusive."""
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)


def query_usage_from_scylla(msisdn, start_date_str, end_date_str):
    """Query HVS (Scylla) for daily data and voice summaries for the msisdn.

    Returns list of rows in the same shape as the Postgres query would return.
    This function will raise if Scylla is unreachable.
    """
    # Parse dates (if provided) else use wide range
    if start_date_str:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    else:
        start_date = datetime(1970, 1, 1).date()

    if end_date_str:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    else:
        end_date = datetime.utcnow().date()

    cluster, session = get_scylla_session()
    try:
        results = []
        # Query data summary per date
        data_q = session.prepare(
            "SELECT event_date, data_type, total_up_bytes, total_down_bytes, total_cost_wak FROM daily_data_summary WHERE msisdn = ? AND event_date = ?"
        )

        voice_q = session.prepare(
            "SELECT event_date, call_type, total_duration_sec, total_cost_wak FROM daily_voice_summary WHERE msisdn = ? AND event_date = ?"
        )

        # Accumulate by date similar to the Postgres grouped output
        for d in iterate_dates(start_date, end_date):
            # data rows
            rows = session.execute(data_q, (msisdn, d))
            for r in rows:
                results.append({
                    "date": r.event_date.isoformat(),
                    "category": "data",
                    "usage_type": r.data_type,
                    "total_up_bytes": getattr(r, 'total_up_bytes', 0),
                    "total_down_bytes": getattr(r, 'total_down_bytes', 0),
                    "total_cost_wak": getattr(r, 'total_cost_wak', 0),
                })

            # voice rows
            rows = session.execute(voice_q, (msisdn, d))
            for r in rows:
                results.append({
                    "date": r.event_date.isoformat(),
                    "category": "call",
                    "usage_type": r.call_type,
                    "total_duration_seconds": getattr(r, 'total_duration_sec', 0),
                    "total_cost_wak": getattr(r, 'total_cost_wak', 0),
                })

        return results
    finally:
        try:
            session.shutdown()
            cluster.shutdown()
        except Exception:
            pass


def execute_query(query, params):
    """Execute query and return results."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


@app.route("/api/usage/<msisdn>", methods=["GET"])
@require_auth
def get_usage_summary(msisdn):
    """Get daily usage summary for an MSISDN."""
    try:
        date_conditions, date_params = build_date_filter(request.args)
        # If USE_HVS is enabled, query Scylla instead
        use_hvs = os.getenv("USE_HVS", "false").lower() in ("1", "true", "yes")
        if use_hvs:
            try:
                scylla_rows = query_usage_from_scylla(msisdn, request.args.get("start_date"), request.args.get("end_date"))
                # Format into API's expected list
                usage = []
                for r in scylla_rows:
                    if r.get("category") == "data":
                        total = int(r.get("total_up_bytes", 0)) + int(r.get("total_down_bytes", 0))
                        usage.append({
                            "category": "data",
                            "usage_type": r.get("usage_type"),
                            "total": total,
                            "measure": "bytes",
                            "start_time": r.get("date")
                        })
                    else:
                        usage.append({
                            "category": "call",
                            "usage_type": r.get("usage_type"),
                            "total": int(r.get("total_duration_seconds", 0)),
                            "measure": "seconds",
                            "start_time": r.get("date")
                        })

                return jsonify({
                    "msisdn": msisdn,
                    "start_date": request.args.get("start_date"),
                    "end_date": request.args.get("end_date"),
                    "usage": usage
                })
            except Exception as e:
                app.logger.warning("Scylla query failed, falling back to Postgres: %s", e)

        # Fallback to Postgres (existing behavior)
        query = """
            SELECT 
                DATE(start_time) as date,
                COUNT(*) as total_calls,
                SUM(CASE WHEN call_type = 'voice' THEN 1 ELSE 0 END) as voice_calls,
                SUM(CASE WHEN call_type = 'video' THEN 1 ELSE 0 END) as video_calls,
                SUM(call_duration_sec) as total_duration_seconds,
                AVG(call_duration_sec) as avg_duration_seconds
            FROM cdr_data.voice_cdr
            WHERE msisdn = %s
        """

        if date_conditions:
            query += " AND " + " AND ".join(date_conditions)

        query += " GROUP BY DATE(start_time) ORDER BY date DESC"

        results = execute_query(query, [msisdn] + date_params)

        return jsonify({
            "msisdn": msisdn,
            "start_date": request.args.get("start_date"),
            "end_date": request.args.get("end_date"),
            "data": [dict(row) for row in results]
        })
        
    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/usage/<msisdn>/summary", methods=["GET"])
@require_auth
def get_usage_aggregate(msisdn):
    """Get overall usage summary for an MSISDN."""
    try:
        date_conditions, date_params = build_date_filter(request.args)
        # If USE_HVS is enabled, try to compute aggregate from Scylla
        use_hvs = os.getenv("USE_HVS", "false").lower() in ("1", "true", "yes")
        if use_hvs:
            try:
                scylla_rows = query_usage_from_scylla(msisdn, request.args.get("start_date"), request.args.get("end_date"))
                # Aggregate
                total_calls = sum(1 for r in scylla_rows if r.get("category") == "call")
                total_duration = sum(int(r.get("total_duration_seconds", 0)) for r in scylla_rows if r.get("category") == "call")
                active_days = len(set(r.get("date") for r in scylla_rows))
                first = min((r.get("date") for r in scylla_rows), default=None)
                last = max((r.get("date") for r in scylla_rows), default=None)

                return jsonify({
                    "msisdn": msisdn,
                    "start_date": request.args.get("start_date"),
                    "end_date": request.args.get("end_date"),
                    "summary": {
                        "total_calls": total_calls,
                        "total_duration_seconds": total_duration,
                        "active_days": active_days,
                        "first_call": first,
                        "last_call": last,
                    }
                })
            except Exception as e:
                app.logger.warning("Scylla aggregate failed, falling back to Postgres: %s", e)

        query = """
            SELECT 
                COUNT(*) as total_calls,
                SUM(CASE WHEN call_type = 'voice' THEN 1 ELSE 0 END) as voice_calls,
                SUM(CASE WHEN call_type = 'video' THEN 1 ELSE 0 END) as video_calls,
                SUM(call_duration_sec) as total_duration_seconds,
                COUNT(DISTINCT DATE(start_time)) as active_days,
                MIN(start_time) as first_call,
                MAX(start_time) as last_call
            FROM cdr_data.voice_cdr
            WHERE msisdn = %s
        """

        if date_conditions:
            query += " AND " + " AND ".join(date_conditions)

        results = execute_query(query, [msisdn] + date_params)

        return jsonify({
            "msisdn": msisdn,
            "start_date": request.args.get("start_date"),
            "end_date": request.args.get("end_date"),
            "summary": dict(results[0]) if results else {}
        })
        
    except Exception as e:
        app.logger.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


def get_app():
    """Return the Flask `app` instance for external use (tests, WSGI, etc.)."""
    return app


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)