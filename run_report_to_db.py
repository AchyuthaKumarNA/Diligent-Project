#!/usr/bin/env python3
"""
Run the SQL in `report.sql` against `ecom.db`, store results in `report_output`.

Steps:
 - Connect to ecom.db
 - Read SQL from report.sql
 - Drop table `report_output` if it exists
 - Create table `report_output` AS the SELECT query
 - Print how many rows were inserted
 - Close the DB connection

Usage:
    python run_report_to_db.py
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("ecom.db")
SQL_FILE = Path("report.sql")


def main():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return
    if not SQL_FILE.exists():
        print(f"SQL file not found: {SQL_FILE}")
        return

    sql_text = SQL_FILE.read_text(encoding="utf-8").strip()
    # Remove trailing semicolon if present to safely append
    if sql_text.endswith(";"):
        sql_text = sql_text[:-1]

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys = ON;")

        # Drop existing report_output if present
        cur.execute("DROP TABLE IF EXISTS report_output;")

        # Create table as the query result
        create_sql = f"CREATE TABLE report_output AS {sql_text};"
        cur.execute(create_sql)
        conn.commit()

        # Count rows inserted
        cur.execute("SELECT COUNT(*) FROM report_output;")
        count = cur.fetchone()[0]
        print(f"Inserted {count} rows into report_output")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
