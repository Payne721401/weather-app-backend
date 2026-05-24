"""Unit tests for ``database/models.py`` batch save logic.

The batch_save methods share a common pattern: chunked Firestore writes
with a quota-aware circuit breaker. These tests cover the happy path and
the three error branches we care about:

1. Normal success — every record written.
2. ResourceExhausted / DeadlineExceeded — raise immediately (circuit breaker).
3. Generic exception — record failure, continue with rest.

The tests mock ``get_firestore_client`` so no real Firestore traffic occurs.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import DeadlineExceeded, ResourceExhausted

from functions.database.models import AirQualityData, ObservationData

pytestmark = pytest.mark.unit


def _dummy_firestore_client(set_side_effect=None):
    """Build a MagicMock Firestore client with a configurable ``batch.set``."""
    dummy_batch = MagicMock()
    dummy_batch.set.side_effect = set_side_effect
    dummy_batch.commit.return_value = None

    dummy_db = MagicMock()
    dummy_db.batch.return_value = dummy_batch
    dummy_db.collection.return_value = dummy_db
    dummy_db.document.return_value = dummy_db
    return dummy_db


@pytest.fixture
def sample_aq_data():
    return [
        {
            "stationId": "001",
            "stationName": "測試站1",
            "county": "台北市",
            "location": {"latitude": 25.0, "longitude": 121.5},
            "measurements": {"pm25": 12.0},
            "publishTime": "2025-11-04T00:00:00Z",
        },
        {
            "stationId": "002",
            "stationName": "測試站2",
            "county": "新北市",
            "location": {"latitude": 25.1, "longitude": 121.6},
            "measurements": {"pm25": 18.0},
            "publishTime": "2025-11-04T00:00:00Z",
        },
    ]


@pytest.fixture
def sample_observation_data():
    return [
        {
            "stationId": "S001",
            "stationName": "測試觀測站",
            "latitude": "25.0",
            "longitude": "121.5",
            "observations": {"temperature": "25.0"},
        }
    ]


class TestAirQualityBatchSave:
    def test_normal_success_writes_both_records(self, sample_aq_data):
        db = _dummy_firestore_client()
        with patch("functions.database.models.get_firestore_client", return_value=db):
            stats = AirQualityData.batch_save(sample_aq_data)
        assert stats["success_count"] == 2
        assert stats["failed_count"] == 0
        assert stats["failed_items"] == []

    def test_quota_error_on_first_record_raises_immediately(self, sample_aq_data):
        db = _dummy_firestore_client(set_side_effect=ResourceExhausted("Quota exceeded"))
        with patch("functions.database.models.get_firestore_client", return_value=db):
            with pytest.raises(ResourceExhausted):
                AirQualityData.batch_save(sample_aq_data)

    def test_quota_error_on_second_record_still_raises(self, sample_aq_data):
        db = _dummy_firestore_client(
            set_side_effect=[None, ResourceExhausted("Quota exceeded")]
        )
        with patch("functions.database.models.get_firestore_client", return_value=db):
            with pytest.raises(ResourceExhausted):
                AirQualityData.batch_save(sample_aq_data)

    def test_deadline_exceeded_raises(self, sample_aq_data):
        db = _dummy_firestore_client(set_side_effect=DeadlineExceeded("Timeout"))
        with patch("functions.database.models.get_firestore_client", return_value=db):
            with pytest.raises(DeadlineExceeded):
                AirQualityData.batch_save(sample_aq_data)

    def test_generic_exception_records_failure_does_not_raise(self, sample_aq_data):
        db = _dummy_firestore_client(set_side_effect=Exception("Some other error"))
        with patch("functions.database.models.get_firestore_client", return_value=db):
            stats = AirQualityData.batch_save(sample_aq_data)
        assert stats["failed_count"] == 2
        assert stats["success_count"] == 0
        assert "Some other error" in stats["failed_items"][0]["error"]

    def test_empty_input_returns_empty_stats(self):
        db = _dummy_firestore_client()
        with patch("functions.database.models.get_firestore_client", return_value=db):
            stats = AirQualityData.batch_save([])
        assert stats["success_count"] == 0
        assert stats["failed_count"] == 0
        assert stats["total_attempts"] == 0


class TestObservationDataBatchSave:
    def test_normal_success(self, sample_observation_data):
        db = _dummy_firestore_client()
        with patch("functions.database.models.get_firestore_client", return_value=db):
            stats = ObservationData.batch_save(sample_observation_data)
        assert stats["success_count"] == 1
        assert stats["failed_count"] == 0

    def test_generic_exception_does_not_abort_remaining(self):
        data = [
            {
                "stationId": "S001",
                "stationName": "Station1",
                "latitude": "25.0",
                "longitude": "121.5",
                "observations": {},
            },
            {
                "stationId": "S002",
                "stationName": "Station2",
                "latitude": "25.1",
                "longitude": "121.6",
                "observations": {},
            },
        ]
        db = _dummy_firestore_client(set_side_effect=[Exception("boom"), None])
        with patch("functions.database.models.get_firestore_client", return_value=db):
            stats = ObservationData.batch_save(data)
        assert stats["failed_count"] == 1
        assert stats["success_count"] == 1
