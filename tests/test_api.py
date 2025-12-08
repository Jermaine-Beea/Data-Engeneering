import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, date
import os
import json

# Import your Flask app + functions
from cdr_usage_api.main import (
    get_app,
    build_date_filter,
    iterate_dates,
    query_usage_from_scylla,
    execute_query,
    get_db_connection,
)

class TestUtilityFunctions(unittest.TestCase):

    def test_build_date_filter(self):
        params = {"start_date": "2025-01-01", "end_date": "2025-01-31"}
        conditions, values = build_date_filter(params)

        self.assertEqual(
            conditions,
            ["DATE(start_time) >= %s", "DATE(start_time) <= %s"]
        )
        self.assertEqual(values, ["2025-01-01", "2025-01-31"])

    def test_iterate_dates(self):
        start = date(2025, 1, 1)
        end = date(2025, 1, 3)

        dates = list(iterate_dates(start, end))
        self.assertEqual(dates, [
            date(2025, 1, 1),
            date(2025, 1, 2),
            date(2025, 1, 3)
        ])


class TestDatabaseQuery(unittest.TestCase):

    @patch("cdr_usage_api.main.get_db_connection")
    def test_execute_query(self, mock_conn):
        # Mock DB connection + cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{"a": 1}]

        mock_conn_obj = MagicMock()
        mock_conn_obj.cursor.return_value = mock_cursor
        mock_conn.return_value = mock_conn_obj

        result = execute_query("SELECT 1", [])

        self.assertEqual(result, [{"a": 1}])
        mock_cursor.execute.assert_called_once()


# class TestScyllaQuery(unittest.TestCase):

    # @patch("cdr_usage_api.main.get_scylla_session", return_value=(None, None))
    # @patch("cdr_usage_api.main.execute_query", return_value=[("12345", "2025-01-01", 50)])
    # def test_query_usage_from_scylla(self, mock_execute_query, mock_get_scylla_session):
    #     # Fake prepared statements
    #     mock_cluster = MagicMock()
    #     mock_sess = MagicMock()

    #     # Fake daily rows
    #     mock_data_row = MagicMock()
    #     mock_data_row.event_date = date(2025, 1, 1)
    #     mock_data_row.data_type = "4G"
    #     mock_data_row.total_up_bytes = 100
    #     mock_data_row.total_down_bytes = 200
    #     mock_data_row.total_cost_wak = 10

    #     mock_voice_row = MagicMock()
    #     mock_voice_row.event_date = date(2025, 1, 1)
    #     mock_voice_row.call_type = "voice"
    #     mock_voice_row.total_duration_sec = 50
    #     mock_voice_row.total_cost_wak = 2

    #     mock_sess.execute.side_effect = [
    #         [mock_data_row],  # data
    #         [mock_voice_row], # voice
    #     ]

    #     mock_sess.return_value = (mock_cluster, mock_sess)

    #     rows = query_usage_from_scylla("12345", "2025-01-01", "2025-01-01")

    #     self.assertEqual(len(rows), 2)
    #     self.assertEqual(rows[0]["category"], "data")
    #     self.assertEqual(rows[1]["category"], "call")


class TestFlaskRoutes(unittest.TestCase):

    def setUp(self):
        app = get_app()
        app.config["TESTING"] = True
        self.client = app.test_client()

    @patch("cdr_usage_api.main.get_db_connection")
    def test_health(self, mock_conn):
        mock_conn.return_value = MagicMock()

        res = self.client.get("/health")
        data = json.loads(res.data)

        self.assertEqual(res.status_code, 200)
        self.assertEqual(data["status"], "healthy")

    def test_usage_requires_auth(self):
        res = self.client.get("/api/usage/12345")
        self.assertEqual(res.status_code, 401)

    # @patch("cdr_usage_api.main.query_usage_from_scylla")
    # def test_usage_fallback_postgres(self, mock_exec):
    #     mock_exec.return_value = [
    #         {"date": "2025-01-01", "total_calls": 2}
    #     ]

    #     res = self.client.get(
    #         "/api/usage/12345",
    #         headers={"Authorization": "Basic YWRtaW46YWRtaW4xMjM="}  # admin:admin123
    #     )

    #     self.assertEqual(res.status_code, 200)
    #     body = json.loads(res.data)
    #     self.assertIn("data", body)
    #     self.assertEqual(len(body["data"]), 1)

    # @patch("cdr_usage_api.main.get_scylla_session")
    # def test_usage_scylla_enabled(self, mock_scylla):
    #     os.environ["USE_HVS"] = "true"

    #     mock_scylla.return_value = [
    #         {
    #             "date": "2025-01-01",
    #             "category": "call",
    #             "usage_type": "voice",
    #             "total_duration_seconds": 50
    #         }
    #     ]

    #     res = self.client.get(
    #         "/api/usage/999",
    #         headers={"Authorization": "Basic YWRtaW46YWRtaW4xMjM="}
    #     )

    #     body = json.loads(res.data)
    #     self.assertIn("usage", body)
    #     self.assertEqual(body["usage"][0]["category"], "call")

    #     del os.environ["USE_HVS"]


if __name__ == "__main__":
    unittest.main()
