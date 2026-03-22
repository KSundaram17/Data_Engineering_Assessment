import csv
import os
import sqlite3

from config import load_config


class BronzeExporter:
    def __init__(self, config_path: str = "config/config_local.yaml") -> None:
        self.config = load_config(config_path)
        self.db_path = self.config["raw_db_path"]
        self.export_dir = self.config["exported_dir_path"]
        self.tables = self.config["bronze_tables"]

    def export_table(self, conn, table_name: str) -> None:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")

        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        os.makedirs(self.export_dir, exist_ok=True)
        file_path = os.path.join(self.export_dir, f"{table_name}.csv")

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns) #header
            writer.writerows(rows)

        print(f"Exported {table_name} -> {file_path}")

    def run(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            for table in self.tables:
                self.export_table(conn, table)
        finally:
            conn.close()


def main() -> None:
    exporter = BronzeExporter()
    exporter.run()


if __name__ == "__main__":
    main()