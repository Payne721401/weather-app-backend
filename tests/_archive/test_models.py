import unittest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
from google.api_core.exceptions import ResourceExhausted, DeadlineExceeded

project_root = Path(__file__).resolve().parent.parent
functions_dir = project_root / 'functions'
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
if str(functions_dir) not in sys.path:
    sys.path.insert(0, str(functions_dir))

from functions.database.models import AirQualityData

class TestAirQualityDataBatchSave(unittest.TestCase):
    def setUp(self):
        self.data = [
            {
                "stationId": "001",
                "stationName": "測試站1",
                "county": "台北市",
                "location": {"latitude": 25.0, "longitude": 121.5},
                "measurements": {},
                "publishTime": "2025-11-04T00:00:00Z"
            },
            {
                "stationId": "002",
                "stationName": "測試站2",
                "county": "新北市",
                "location": {"latitude": 25.1, "longitude": 121.6},
                "measurements": {},
                "publishTime": "2025-11-04T00:00:00Z"
            }
        ]

    def test_normal_success(self):
        # 模擬全部成功
        dummy_batch = MagicMock()
        dummy_batch.set.return_value = None
        dummy_batch.commit.return_value = None

        dummy_db = MagicMock()
        dummy_db.batch.return_value = dummy_batch
        dummy_db.collection.return_value = dummy_db
        dummy_db.document.return_value = dummy_db

        with patch("functions.database.models.get_firestore_client", return_value=dummy_db):
            stats = AirQualityData.batch_save(self.data)
            self.assertEqual(stats['success_count'], 2)
            self.assertEqual(stats['failed_count'], 0)

    def test_quota_error_first(self):
        # 第一筆就 quota error
        dummy_batch = MagicMock()
        dummy_batch.set.side_effect = ResourceExhausted("Quota exceeded")
        dummy_batch.commit.return_value = None

        dummy_db = MagicMock()
        dummy_db.batch.return_value = dummy_batch
        dummy_db.collection.return_value = dummy_db
        dummy_db.document.return_value = dummy_db

        with patch("functions.database.models.get_firestore_client", return_value=dummy_db):
            with self.assertRaises(ResourceExhausted):
                AirQualityData.batch_save(self.data)

    def test_quota_error_second(self):
        # 第二筆 quota error
        dummy_batch = MagicMock()
        dummy_batch.set.side_effect = [None, ResourceExhausted("Quota exceeded")]
        dummy_batch.commit.return_value = None

        dummy_db = MagicMock()
        dummy_db.batch.return_value = dummy_batch
        dummy_db.collection.return_value = dummy_db
        dummy_db.document.return_value = dummy_db

        with patch("functions.database.models.get_firestore_client", return_value=dummy_db):
            with self.assertRaises(ResourceExhausted):
                AirQualityData.batch_save(self.data)

    def test_deadline_exceeded(self):
        # 第一筆就 DeadlineExceeded
        dummy_batch = MagicMock()
        dummy_batch.set.side_effect = DeadlineExceeded("Timeout")
        dummy_batch.commit.return_value = None

        dummy_db = MagicMock()
        dummy_db.batch.return_value = dummy_batch
        dummy_db.collection.return_value = dummy_db
        dummy_db.document.return_value = dummy_db

        with patch("functions.database.models.get_firestore_client", return_value=dummy_db):
            with self.assertRaises(DeadlineExceeded):
                AirQualityData.batch_save(self.data)

    def test_general_exception(self):
        # 一般 Exception 不 raise
        dummy_batch = MagicMock()
        dummy_batch.set.side_effect = Exception("Some other error")
        dummy_batch.commit.return_value = None

        dummy_db = MagicMock()
        dummy_db.batch.return_value = dummy_batch
        dummy_db.collection.return_value = dummy_db
        dummy_db.document.return_value = dummy_db

        with patch("functions.database.models.get_firestore_client", return_value=dummy_db):
            stats = AirQualityData.batch_save(self.data)
            self.assertEqual(stats['failed_count'], 2)
            self.assertIn("Some other error", stats['failed_items'][0]['error'])

if __name__ == '__main__':
    unittest.main()