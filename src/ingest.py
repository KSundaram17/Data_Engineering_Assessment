import csv
import hashlib
import json
import sqlite3
from datetime import datetime, timezone

from config import load_config


class Ingestion:
    def __init__(self, config_path: str = "config/config_local.yaml") -> None:
        self.config = load_config(config_path)

        self.jobs_path = self.config["input_jobs"]
        self.candidates_path = self.config["input_candidates"]
        self.workflow_events_path = self.config["input_workflow_events"]
        self.education_path = self.config["input_education"]
        self.applications_path = self.config["input_applications"]

        self.bronze_db_path = self.config["raw_db_path"]
        self.loaddatetime = datetime.now(timezone.utc).isoformat()

    def get_connection(self):
        conn = sqlite3.connect(self.bronze_db_path)
        cursor = conn.cursor()
        return conn, cursor

    def normalize_date(self, date_str):
        if not date_str:
            return None

        date_str = date_str.strip()

        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y.%m.%d",
            "%B %d, %Y",
            "%d-%b-%Y",
            "%Y-%m-%dT%H:%M:%S",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        print(f"Date parsing failed for: {date_str}")
        return None

    def ingest_jobs(self, cursor, conn):
        print("Starting jobs ingestion...")

        with open(self.jobs_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row in reader:
                job_id = row.get("job_id")
                title = row.get("title")
                department = row.get("department")
                posted_date = row.get("posted_date")
                status = row.get("status")

                job_id = job_id.strip() if job_id else None
                title = title.strip() if title else None
                department = department.strip() if department else None
                status = status.strip() if status else None

                posted_date = self.normalize_date(posted_date)

                cursor.execute(
                    """
                    INSERT OR REPLACE INTO bronze_jobs
                    (job_id, title, department, posted_date, status, loaddatetime)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (job_id, title, department, posted_date, status, self.loaddatetime),
                )

        conn.commit()
        print("Jobs ingestion complete")

    def ingest_candidates(self, cursor, conn):
        print("Starting candidates ingestion...")

        with open(self.candidates_path, "r", encoding="utf-8") as file:
            candidates = json.load(file)

            for candidate in candidates:
                candidate_id = candidate.get("candidate_id")
                first_name = candidate.get("first_name")
                last_name = candidate.get("last_name")
                email = candidate.get("email")
                phone = candidate.get("phone")
                skills = candidate.get("skills")

                candidate_id = candidate_id.strip() if candidate_id else None
                first_name = first_name.strip() if first_name else None
                last_name = last_name.strip() if last_name else None
                email = email.strip() if email else None
                phone = phone.strip() if phone else None
                skills = json.dumps(skills) if skills else None

                cursor.execute(
                    """
                    INSERT OR REPLACE INTO bronze_candidates
                    (candidate_id, first_name, last_name, email, phone, skills, loaddatetime)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate_id,
                        first_name,
                        last_name,
                        email,
                        phone,
                        skills,
                        self.loaddatetime,
                    ),
                )

        conn.commit()
        print("Candidates ingestion complete")

    def ingest_education(self, cursor, conn):
        print("Starting education ingestion...")

        with open(self.education_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row in reader:
                candidate_id = row.get("candidate_id")
                degree = row.get("degree")
                institution = row.get("institution")
                year = row.get("year")

                candidate_id = candidate_id.strip() if candidate_id else None
                degree = degree.strip() if degree else None
                institution = institution.strip() if institution else None
                year = year.strip() if year else None

                cursor.execute(
                    """
                    INSERT OR REPLACE INTO bronze_education
                    (candidate_id, degree, institution, year, loaddatetime)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        candidate_id,
                        degree,
                        institution,
                        year,
                        self.loaddatetime,
                    ),
                )

        conn.commit()
        print("Education ingestion complete")

    def ingest_applications(self, cursor, conn):
        print("Starting applications ingestion...")

        with open(self.applications_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row in reader:
                application_id = row.get("application_id")
                candidate_id = row.get("candidate_id")
                job_id = row.get("job_id")
                apply_date = row.get("apply_date")

                application_id = application_id.strip() if application_id else None
                candidate_id = candidate_id.strip() if candidate_id else None
                job_id = job_id.strip() if job_id else None

                apply_date = self.normalize_date(apply_date)

                cursor.execute(
                    """
                    INSERT OR REPLACE INTO bronze_applications
                    (application_id, candidate_id, job_id, apply_date, loaddatetime)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        application_id,
                        candidate_id,
                        job_id,
                        apply_date,
                        self.loaddatetime,
                    ),
                )

        conn.commit()
        print("Applications ingestion complete")

    def ingest_workflow_events(self, cursor, conn):
        print("Starting workflow events ingestion...")

        with open(self.workflow_events_path, "r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue

                row = json.loads(line)

                application_id = row.get("application_id")
                old_status = row.get("old_status")
                new_status = row.get("new_status")
                event_timestamp = row.get("event_timestamp")

                application_id = application_id.strip() if application_id else None
                old_status = old_status.strip() if old_status else None
                new_status = new_status.strip() if new_status else None
                event_timestamp = event_timestamp.strip() if event_timestamp else None

                event_timestamp = self.normalize_date(event_timestamp)

                event_id = hashlib.sha256(
                    f"{application_id}_{old_status}_{new_status}_{event_timestamp}".encode()
                ).hexdigest()

                cursor.execute(
                    """
                    INSERT OR REPLACE INTO bronze_workflow_events
                    (event_id, application_id, old_status, new_status, event_timestamp, loaddatetime)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        application_id,
                        old_status,
                        new_status,
                        event_timestamp,
                        self.loaddatetime,
                    ),
                )

        conn.commit()
        print("Workflow events ingestion complete")


if __name__ == "__main__":
    ingestion = Ingestion()
    conn, cursor = ingestion.get_connection()

    try:
        ingestion.ingest_jobs(cursor, conn)
        ingestion.ingest_candidates(cursor, conn)
        ingestion.ingest_education(cursor, conn)
        ingestion.ingest_applications(cursor, conn)
        ingestion.ingest_workflow_events(cursor, conn)
    finally:
        conn.close()