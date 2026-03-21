import sqlite3
import csv
import os

DB_PATH = "output/bronze.db"
EXPORT_DIR = "exported_bronze"

TABLES = [
    "bronze_jobs",
    "bronze_candidates",
    "bronze_education",
    "bronze_applications",
    "bronze_workflow_events"
]

def export_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    os.makedirs(EXPORT_DIR, exist_ok=True)
    file_path = os.path.join(EXPORT_DIR, f"{table_name}.csv")

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)   # header
        writer.writerows(rows)

    print(f"Exported {table_name} → {file_path}")


def main():
    conn = sqlite3.connect(DB_PATH)

    for table in TABLES:
        export_table(conn, table)

    conn.close()


if __name__ == "__main__":
    main()