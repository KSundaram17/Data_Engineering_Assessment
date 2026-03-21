import sqlite3
import csv
from datetime import datetime,timezone
import json
import hashlib

class Ingestion:
    def __init__(self, source):
        self.source = source
    
    def get_connection(self):
       conn = sqlite3.connect("output/bronze.db")
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
            "%Y-%m-%dT%H:%M:%S"
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue  # try next format

        # only if ALL formats failed provide the error message
        print(f"Date parsing failed for: {date_str}")
        return None


    def ingest_jobs(self, cursor, conn):
        print("Starting jobs ingestion...")
        # loaddatetime -MOVE IT TO INIT
        loaddatetime = datetime.now(timezone.utc).isoformat()
        with open(r"datasets/jobs.csv", "r", encoding="utf-8") as file: #CHANGE THE PATH TO SOURCE
            reader = csv.DictReader(file)

            for row in reader:
                # extract
                job_id = row.get("job_id")
                title = row.get("title")
                department = row.get("department")
                posted_date = row.get("posted_date")
                status = row.get("status")

                # clean
                job_id = job_id.strip() if job_id else None
                title = title.strip() if title else None
                department = department.strip() if department else None
                status = status.strip() if status else None

                # normalize date
                posted_date = self.normalize_date(posted_date)

                # insert (idempotent)
                cursor.execute("""
                    INSERT OR REPLACE INTO bronze_jobs
                    (job_id, title, department, posted_date, status, loaddatetime)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (job_id, title, department, posted_date, status, loaddatetime))

        conn.commit()
        print("Jobs ingestion complete")
    
    def ingest_candidates(self, cursor, conn):
        print("Starting candidates ingestion...")
        with open(r"datasets/candidates.json", "r", encoding = "utf-8") as file: #CHANGE THE PATH TO SOURCE
            candidates = json.load(file)
            # loaddatetime - MOVE IT TO INIT
            loaddatetime = datetime.now(timezone.utc).isoformat()
            
            for candidate in candidates:
                # extract
                candidate_id = candidate.get("candidate_id")
                first_name = candidate.get("first_name")
                last_name = candidate.get("last_name")
                email = candidate.get("email")
                phone = candidate.get("phone")
                skills = candidate.get("skills")

                # clean
                candidate_id = candidate_id.strip() if candidate_id else None
                first_name = first_name.strip() if first_name else None
                last_name = last_name.strip() if last_name else None
                email = email.strip() if email else None
                phone = phone.strip() if phone else None
                # Convert the 'skills' column to JSON strings
                skills = json.dumps(skills) if skills else None

                # insert (idempotent)
                cursor.execute("""
                    INSERT OR REPLACE INTO bronze_candidates
                    (candidate_id, first_name, last_name, email, phone, skills, loaddatetime)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (candidate_id, first_name, last_name, email, phone, skills, loaddatetime))

        conn.commit()
        print("Candidates ingestion complete")
    
    def ingest_education(self, cursor, conn):
        print("Starting education ingestion...")
        with open(r"datasets/education.csv", "r", encoding="utf-8") as file: #CHANGE THE PATH TO SOURCE
            reader = csv.DictReader(file)
            # loaddatetime - MOVE IT TO INIT
            loaddatetime = datetime.now(timezone.utc).isoformat()

            for row in reader:
                # extract
                candidate_id = row.get("candidate_id")
                degree = row.get("degree")
                institution = row.get("institution")
                year = row.get("year")

                # clean
                candidate_id = candidate_id.strip() if candidate_id else None
                degree = degree.strip() if degree else None
                institution = institution.strip() if institution else None
                year = year.strip() if year else None

                # insert (idempotent)
                cursor.execute("""
                    INSERT OR REPLACE INTO bronze_education
                    (candidate_id, degree, institution, year, loaddatetime)
                    VALUES (?, ?, ?, ?, ?)
                """, (candidate_id, degree, institution, year, loaddatetime))

        conn.commit()
        print("Education ingestion complete")
    
    def ingest_applications(self, cursor, conn):
        print("Starting applications ingestion...")
        with open(r"datasets/applications.csv", "r", encoding="utf-8") as file: #CHANGE THE PATH TO SOURCE
            reader = csv.DictReader(file)
            # loaddatetime - MOVE IT TO INIT
            loaddatetime = datetime.now(timezone.utc).isoformat()

            for row in reader:
                # extract
                application_id = row.get("application_id")
                candidate_id = row.get("candidate_id")
                job_id = row.get("job_id")
                apply_date = row.get("apply_date")

                # clean
                application_id = application_id.strip() if application_id else None
                candidate_id = candidate_id.strip() if candidate_id else None
                job_id = job_id.strip() if job_id else None

                # normalize date
                apply_date = self.normalize_date(apply_date)

                # insert (idempotent)
                cursor.execute("""
                    INSERT OR REPLACE INTO bronze_applications
                    (application_id, candidate_id, job_id, apply_date, loaddatetime)
                    VALUES (?, ?, ?, ?, ?)
                """, (application_id, candidate_id, job_id, apply_date, loaddatetime))

        conn.commit()
        print("Applications ingestion complete")
    
    def ingest_workflow_events(self, cursor, conn):
        print("Starting workflow events ingestion...")
        with open(r"datasets/workflow_events.jsonl", "r", encoding="utf-8") as file: #CHANGE THE PATH TO SOURCE
            # loaddatetime - MOVE IT TO INIT
            loaddatetime = datetime.now(timezone.utc).isoformat()

            for line in file:
                if not line.strip():
                    continue  # skip empty lines
                row = json.loads(line)
                # extract
                application_id = row.get("application_id")
                old_status = row.get("old_status")
                new_status = row.get("new_status")
                event_timestamp = row.get("event_timestamp")

                # clean
                application_id = application_id.strip() if application_id else None
                old_status = old_status.strip() if old_status else None
                new_status = new_status.strip() if new_status else None
                event_timestamp = event_timestamp.strip() if event_timestamp else None

                # normalize date
                event_timestamp = self.normalize_date(event_timestamp)

                # generate event_id
                event_id = hashlib.sha256(f"{application_id}_{old_status}_{new_status}_{event_timestamp}".encode()).hexdigest()

                # insert (idempotent)
                cursor.execute("""
                    INSERT OR REPLACE INTO bronze_workflow_events
                    (event_id, application_id, old_status, new_status, event_timestamp, loaddatetime)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (event_id, application_id, old_status, new_status, event_timestamp, loaddatetime))

        conn.commit()
        print("Workflow events ingestion complete")

if __name__ == "__main__":
    ingestion = Ingestion("datasets")

    conn, cursor = ingestion.get_connection()

    ingestion.ingest_jobs(cursor, conn)
    ingestion.ingest_candidates(cursor, conn)
    ingestion.ingest_education(cursor, conn)
    ingestion.ingest_applications(cursor, conn)
    ingestion.ingest_workflow_events(cursor, conn)

    conn.close()