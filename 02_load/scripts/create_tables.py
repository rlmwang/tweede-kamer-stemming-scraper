import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

env_path = Path.cwd() / ".env"
load_dotenv(dotenv_path=env_path, override=True)


def create_tables(sql_dir="schemas"):
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
    conn.autocommit = True
    cur = conn.cursor()

    sql_files = sorted(f for f in os.listdir(sql_dir) if f.endswith(".sql"))

    for fname in sql_files:
        path = os.path.join(sql_dir, fname)
        with open(path, "r", encoding="utf-8") as f:
            sql = f.read()
        try:
            print(f"Running migration: {fname}")
            cur.execute(sql)
        except psycopg2.Error as e:
            print(f"Warning: failed to run {fname}: {e.pgerror}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    create_tables()
