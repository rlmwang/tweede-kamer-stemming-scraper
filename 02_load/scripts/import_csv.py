import csv
import os
from pathlib import Path

import click
import polars as pl
import psycopg2
from dateparser import parse as parse_date
from dotenv import load_dotenv
from tqdm import tqdm

CSV_FILE_ORDER = ["stemming.csv", "motie.csv", "indieners.csv", "details.csv"]


# Load top-level .env
env_path = Path.cwd() / "default.env"
load_dotenv(dotenv_path=env_path, override=True)


def load_csv_to_table(conn, csv_path: str, table_name: str):
    df = pl.read_csv(csv_path, encoding="utf-8")

    # Convert date-like columns
    df = df.with_columns(
        pl.col(col).map_elements(
            function=lambda x: parse_date(x, languages=["nl"]).date() if x else None,
            return_dtype=pl.Date(),
        )
        for col in df.columns
        if "datum" in col.lower() or "date" in col.lower()
    )

    pk_cols = get_primary_key_columns(conn, table_name)

    with conn.cursor() as cur:
        for row in df.iter_rows(named=True):
            row_dict = dict(row)

            # Drop PK columns that are None -> let DB default apply
            cols = [c for c, v in row_dict.items() if not (c in pk_cols and v is None)]
            vals = [row_dict[c] for c in cols]

            placeholders = ",".join(["%s"] * len(cols))
            insert_sql = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})"

            cur.execute(insert_sql, vals)

    conn.commit()


def get_primary_key_columns(conn, table_name: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a
              ON a.attrelid = i.indrelid
             AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
              AND i.indisprimary;
        """, (table_name,))
        return [r[0] for r in cur.fetchall()]


@click.command()
@click.argument("data_dir", type=click.Path(exists=True, file_okay=False, dir_okay=True))
def main(data_dir):
    """Load CSV files from DATA_DIR into Postgres."""
    data_dir = Path(data_dir)

    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )

    # Walk through all stemming folders
    dirs = [p.parent for p in data_dir.glob("**/stemming.csv")]
    for path in tqdm(dirs):
        if not path.is_dir():
            raise ValueError(f"folder is missing ({path})")

        for csv_file in CSV_FILE_ORDER:
            csv_path = path / csv_file
            table_name = csv_file.replace(".csv", "")
            load_csv_to_table(conn, csv_path, table_name)

    conn.close()


if __name__ == "__main__":
    main()
