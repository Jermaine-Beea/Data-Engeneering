import sys
import unittest
from unittest.mock import MagicMock
import datetime

sys.modules['kafka'] = MagicMock()
sys.modules['kafka.KafkaConsumer'] = MagicMock()
sys.modules['sqlalchemy'] = MagicMock()
sys.modules['sqlalchemy.create_engine'] = MagicMock()
sys.modules['sqlalchemy.orm'] = MagicMock()

import forex_consumer.forex_consumer as fc

class TestForexConsumer(unittest.TestCase):

    def test_insert_batch_empty(self):
        """insert_batch should do nothing if batch is empty"""
        mock_session = MagicMock()
        fc.insert_batch(mock_session, [])
        mock_session.execute.assert_not_called()
        mock_session.commit.assert_not_called()

    def test_insert_batch_nonempty(self):
        """insert_batch should execute insert for each tick"""
        mock_session = MagicMock()
        batch = [
            {"timestamp": datetime.datetime(2025,12,7,12,0), "pair_name":"WAKMRV", "bid_price":100.0, "ask_price":101.0, "spread":1.0},
            {"timestamp": datetime.datetime(2025,12,7,12,1), "pair_name":"MRVZAR", "bid_price":200.0, "ask_price":201.0, "spread":1.0}
        ]
        fc.insert_batch(mock_session, batch)
        self.assertEqual(mock_session.execute.call_count, 2)
        mock_session.commit.assert_called_once()

if __name__ == "__main__":
    unittest.main()
