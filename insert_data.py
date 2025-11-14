#!/usr/bin/env python3
"""
Insert CSV data into an SQLite database `ecom.db`.

Requirements implemented:
- Create tables for products, customers, orders, reviews, categories
- Enable foreign keys (PRAGMA foreign_keys = ON)
- Use INSERT OR IGNORE to avoid duplicates
- Print number of rows inserted per table
- Close DB connection at end

Run:
    python insert_data.py

"""
import csv
import sqlite3
from pathlib import Path
from typing import Optional

DATA_FILES = {
    "categories": "categories.csv",
    "products": "products.csv",
    "customers": "customers.csv",
    "orders": "orders.csv",
    "reviews": "reviews.csv",
}

DB_PATH = Path("ecom.db")


def to_int(v: Optional[str]) -> Optional[int]:
    if v is None or v == "":
        return None
    return int(v)


def to_float(v: Optional[str]) -> Optional[float]:
    if v is None or v == "":
        return None
    return float(v)


def create_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
            ID INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL,
            parent_category_id INTEGER,
            FOREIGN KEY(parent_category_id) REFERENCES categories(ID)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            ID INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category INTEGER,
            price REAL,
            stock_quantity INTEGER,
            FOREIGN KEY(category) REFERENCES categories(ID)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            ID INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            registration_date TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            ID INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            order_date TEXT,
            quantity INTEGER,
            total_price REAL,
            FOREIGN KEY(customer_id) REFERENCES customers(ID),
            FOREIGN KEY(product_id) REFERENCES products(ID)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS reviews (
            ID INTEGER PRIMARY KEY,
            product_id INTEGER,
            customer_id INTEGER,
            rating INTEGER,
            review_text TEXT,
            review_date TEXT,
            FOREIGN KEY(product_id) REFERENCES products(ID),
            FOREIGN KEY(customer_id) REFERENCES customers(ID)
        )
        """
    )

    conn.commit()


def insert_csv(conn: sqlite3.Connection, table: str, file_path: Path) -> int:
    """Insert rows from CSV into `table`. Returns number of rows inserted."""
    cur = conn.cursor()

    # Count before
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    before = cur.fetchone()[0]

    file_path = Path(file_path)
    if not file_path.exists():
        print(f"Warning: {file_path} not found, skipping {table}.")
        return 0

    with file_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    if table == "categories":
        q = "INSERT OR IGNORE INTO categories (ID, category_name, parent_category_id) VALUES (?, ?, ?)"
        params = []
        for r in rows:
            pid = to_int(r.get("parent_category_id"))
            params.append((to_int(r.get("ID")), r.get("category_name"), pid))

    elif table == "products":
        q = "INSERT OR IGNORE INTO products (ID, name, category, price, stock_quantity) VALUES (?, ?, ?, ?, ?)"
        params = []
        for r in rows:
            params.append((to_int(r.get("ID")), r.get("name"), to_int(r.get("category")), to_float(r.get("price")), to_int(r.get("stock_quantity"))))

    elif table == "customers":
        q = "INSERT OR IGNORE INTO customers (ID, name, email, registration_date) VALUES (?, ?, ?, ?)"
        params = []
        for r in rows:
            params.append((to_int(r.get("ID")), r.get("name"), r.get("email"), r.get("registration_date")))

    elif table == "orders":
        q = "INSERT OR IGNORE INTO orders (ID, customer_id, product_id, order_date, quantity, total_price) VALUES (?, ?, ?, ?, ?, ?)"
        params = []
        for r in rows:
            params.append((to_int(r.get("ID")), to_int(r.get("customer_id")), to_int(r.get("product_id")), r.get("order_date"), to_int(r.get("quantity")), to_float(r.get("total_price"))))

    elif table == "reviews":
        q = "INSERT OR IGNORE INTO reviews (ID, product_id, customer_id, rating, review_text, review_date) VALUES (?, ?, ?, ?, ?, ?)"
        params = []
        for r in rows:
            params.append((to_int(r.get("ID")), to_int(r.get("product_id")), to_int(r.get("customer_id")), to_int(r.get("rating")), r.get("review_text"), r.get("review_date")))

    else:
        raise ValueError(f"Unknown table {table}")

    # Insert rows in a transaction
    cur.executemany(q, params)
    conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {table}")
    after = cur.fetchone()[0]
    inserted = after - before
    return inserted


def main():
    conn = sqlite3.connect(str(DB_PATH))
    try:
        create_tables(conn)

        results = {}
        # Insert in order that respects FK dependencies
        order = ["categories", "products", "customers", "orders", "reviews"]
        for t in order:
            file_name = DATA_FILES.get(t)
            inserted = insert_csv(conn, t, Path(file_name))
            results[t] = inserted

        print("Insertion summary:")
        for t, cnt in results.items():
            print(f"- {t}: {cnt} rows inserted")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
