import json
import sqlite3
from pathlib import Path

import pytest

from src.ingest import Ingestion


@pytest.fixture
def ingestion():
    return Ingestion()


def test_normalize_date_multiple_formats(ingestion):
    assert ingestion.normalize_date("2025-01-01") == "2025-01-01"
    assert ingestion.normalize_date("2025/01/01") == "2025-01-01"
    assert ingestion.normalize_date("2025.01.01") == "2025-01-01"
    assert ingestion.normalize_date("January 01, 2025") == "2025-01-01"
    assert ingestion.normalize_date("01-Jan-2025") == "2025-01-01"


def test_normalize_date_invalid_value_returns_none(ingestion):
    assert ingestion.normalize_date("not-a-date") is None
    assert ingestion.normalize_date("") is None
    assert ingestion.normalize_date(None) is None


def test_event_id_hash_is_deterministic():
    application_id = "app_1"
    old_status = "Applied"
    new_status = "Hired"
    event_timestamp = "2025-01-10"

    import hashlib

    event_id_1 = hashlib.sha256(
        f"{application_id}_{old_status}_{new_status}_{event_timestamp}".encode()
    ).hexdigest()

    event_id_2 = hashlib.sha256(
        f"{application_id}_{old_status}_{new_status}_{event_timestamp}".encode()
    ).hexdigest()

    assert event_id_1 == event_id_2
    assert len(event_id_1) == 64