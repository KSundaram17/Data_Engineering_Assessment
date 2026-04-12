from datetime import datetime


def test_time_to_hire_days_calculation():
    apply_date = datetime.strptime("2025-01-01", "%Y-%m-%d").date()
    hired_date = datetime.strptime("2025-01-10", "%Y-%m-%d").date()

    time_to_hire_days = (hired_date - apply_date).days

    assert time_to_hire_days == 9


def test_hired_before_applied_anomaly_detection():
    apply_date = datetime.strptime("2025-01-10", "%Y-%m-%d").date()
    hired_date = datetime.strptime("2025-01-01", "%Y-%m-%d").date()

    is_anomaly = hired_date < apply_date

    assert is_anomaly is True