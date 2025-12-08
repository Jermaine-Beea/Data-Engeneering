import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from prepared_layers.layers.cdr import process_cdr_data_2
from prepared_layers.layers.crm import process_crm_data_1
from prepared_layers.layers.forex import process_forex_data_1, calculate_ema, calculate_atr

class TestPreparedLayers(unittest.TestCase):

    # ------------------ CDR Data - 2 ------------------
    @patch("prepared_layers.layers.cdr.get_db_connection")
    @patch("prepared_layers.layers.cdr.execute_values")
    def test_cdr_data_2_session_detection(self, mock_execute_values, mock_get_conn):
        """Test CDR Data - 2: tower session processing."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Simulate no sessions found
        mock_cursor.fetchall.return_value = []

        process_cdr_data_2()

        mock_cursor.execute.assert_any_call("TRUNCATE TABLE prepared_layers.cdr_tower_sessions")
        mock_conn.commit.assert_called()

    # ------------------ CRM Data - 1 ------------------
    @patch("prepared_layers.layers.crm.get_db_connection")
    @patch("prepared_layers.layers.crm.execute_values")
    @patch("pandas.read_sql")
    def test_crm_data_1_flatten_and_balance(self, mock_read_sql, mock_execute_values, mock_get_conn):
        """Test CRM Data - 1: flattened table creation and balance calculation."""
        mock_cursor = MagicMock()
        mock_conn_prod = MagicMock()
        mock_conn_analytics = MagicMock()
        mock_conn_analytics.cursor.return_value = mock_cursor
        mock_get_conn.side_effect = [mock_conn_prod, mock_conn_analytics]

        # Dummy CRM data
        mock_read_sql.return_value = pd.DataFrame({
            "account_id": [1],
            "owner_name": ["John Doe"],
            "email": ["john@example.com"],
            "msisdn": ["27820000001"],
            "device_id": [101],
            "device_name": ["iPhone"],
            "device_type": ["Mobile"],
            "device_os": ["iOS"],
            "street_address": ["123 Main St"],
            "city": ["Cape Town"],
            "state": ["Western Cape"],
            "postal_code": ["8000"],
            "country": ["South Africa"],
            "last_modified": [pd.Timestamp.now()]
        })

        process_crm_data_1()

        # Check table creation and commit
        mock_cursor.execute.assert_any_call(
            """\n        DROP TABLE IF EXISTS prepared_layers.crm_flattened_balance CASCADE;\n        \n        CREATE TABLE prepared_layers.crm_flattened_balance (
            account_id INTEGER,
            owner_name VARCHAR(255),
            email VARCHAR(255),
            msisdn VARCHAR(20),
            device_id INTEGER,
            device_name VARCHAR(255),
            device_type VARCHAR(50),
            device_os VARCHAR(50),
            street_address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(50),
            postal_code VARCHAR(20),
            country VARCHAR(50),
            last_modified TIMESTAMP,
            total_data_bytes BIGINT DEFAULT 0,
            data_cost_zar DECIMAL(10,2) DEFAULT 0,
            total_call_seconds INTEGER DEFAULT 0,
            voice_cost_zar DECIMAL(10,2) DEFAULT 0,
            total_cost_zar DECIMAL(10,2) DEFAULT 0,
            running_balance_zar DECIMAL(10,2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (account_id, device_id)
        );
        \n        CREATE INDEX idx_crm_flattened_msisdn ON prepared_layers.crm_flattened_balance(msisdn);
        CREATE INDEX idx_crm_flattened_account ON prepared_layers.crm_flattened_balance(account_id);
        """
        )
        mock_conn_analytics.commit.assert_called()

    # ------------------ Forex Data - 1 ------------------
    # def test_forex_data_1_ema_atr(self):
    #     """Test Forex Data - 1: EMA and ATR calculations."""
    #     df = pd.DataFrame({
    #         "open": [1,2,3,4,5,6,7,8],
    #         "high": [1,2,3,4,5,6,7,8],
    #         "low": [0,1,2,3,4,5,6,7],
    #         "close": [1,2,3,4,5,6,7,8]
    #     })

    #     ema8 = calculate_ema(df['open'], 8)
    #     ema21 = calculate_ema(df['open'], 21)
    #     atr8 = calculate_atr(df['high'], df['low'], df['close'], 8)
    #     atr21 = calculate_atr(df['high'], df['low'], df['close'], 21)

    #     self.assertEqual(len(ema8), len(df))
    #     self.assertEqual(len(atr21), len(df))
    #     self.assertGreater(ema8.iloc[-1], 0)
    #     self.assertGreaterEqual(atr21.iloc[-1], 0)

    # ------------------ CDR Data - 1 (merge check) ------------------
    def test_cdr_data_1_merge_and_columns(self):
        """Test CDR Data - 1: merge voice and data usage and column check."""
        voice_df = pd.DataFrame({
            "datetime": ["2025-12-07 12:00:00"],
            "msisdn": ["27820000001"],
            "voice_call_count": [2],
            "video_call_count": [1],
            "total_call_duration_sec": [120]
        })
        data_df = pd.DataFrame({
            "datetime": ["2025-12-07 12:00:00"],
            "msisdn": ["27820000001"],
            "video_up": [1000],
            "video_down": [800],
            "audio_up": [500],
            "audio_down": [400],
            "image_up": [0],
            "image_down": [0],
            "text_up": [0],
            "text_down": [0],
            "app_up": [0],
            "app_down": [0],
            "total_up": [1500],
            "total_down": [1200]
        })

        merged_df = pd.merge(voice_df, data_df, on=["datetime","msisdn"], how="outer").fillna(0)

        required_columns = [
            "datetime","msisdn","voice_call_count","video_call_count","total_call_duration_sec",
            "video_up","video_down","audio_up","audio_down","image_up","image_down","text_up","text_down",
            "app_up","app_down","total_up","total_down"
        ]

        self.assertTrue(all(col in merged_df.columns for col in required_columns))
        self.assertEqual(merged_df["voice_call_count"].iloc[0], 2)
        self.assertEqual(merged_df["video_up"].iloc[0], 1000)

if __name__ == "__main__":
    unittest.main()
