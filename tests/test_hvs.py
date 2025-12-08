import sys, datetime
from unittest.mock import MagicMock

# Mock kafka, cassandra, prometheus_client so they do not actually load
sys.modules['kafka'] = MagicMock()
sys.modules['kafka.KafkaConsumer'] = MagicMock()
sys.modules['cassandra'] = MagicMock()
sys.modules['cassandra.cluster'] = MagicMock()
sys.modules['prometheus_client'] = MagicMock()

import hvs.consumer as consumer

import unittest

class TestConsumerHelpers(unittest.TestCase):
    def test_to_usage_date(self):
        ts = "2025-12-07T12:34:56Z"
        date = consumer.to_usage_date(ts)
        self.assertEqual(date, datetime.date(2025,12,7))

    def test_data_cost_wak(self):
        cost = consumer.data_cost_wak(1000000000)
        expected = int(round(49.0 * consumer.WAK_PER_ZAR))
        self.assertEqual(cost, expected)

    def test_voice_cost_wak(self):
        cost = consumer.voice_cost_wak(60)
        expected = int(round(1 * consumer.WAK_PER_ZAR))
        self.assertEqual(cost, expected)

if __name__ == "__main__":
    unittest.main()
