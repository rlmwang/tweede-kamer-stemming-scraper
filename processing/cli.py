#!/usr/bin/env python
import os
import subprocess as sp
from pathlib import Path

import click
from dotenv import load_dotenv

# Load top-level .env
env_path = Path.cwd() / ".env"
load_dotenv(dotenv_path=env_path, override=True)

POSTGRES_CONTAINER_NAME = os.getenv("POSTGRES_CONTAINER_NAME", "tweede-kamer-db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "stemmer")
POSTGRES_DB = os.getenv("POSTGRES_DB", "stemmen")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")


@click.group()
def cli():
    """Manage Postgres and data for stemmingsuitslagen."""
    pass


@cli.command()
def start():
    click.echo("Starting Postgres...")
    sp.run(f"bash scripts/start_postgres.sh", shell=True, check=True)


@cli.command()
def stop():
    click.echo("Stopping Postgres...")
    sp.run(
        f"docker stop $(docker ps -q --filter 'name={POSTGRES_CONTAINER_NAME}') || true",
        shell=True,
        check=True,
    )


@cli.command()
def logs():
    sp.run(
        f"docker logs -f $(docker ps -q --filter 'name={POSTGRES_CONTAINER_NAME}')",
        shell=True,
        check=True,
    )


@cli.command()
def status():
    sp.run(f"docker ps --filter 'name={POSTGRES_CONTAINER_NAME}'", shell=True, check=True)


@cli.command("create-db")
def create_db():
    sp.run(f"uv run python scripts/create_tables.py", shell=True, check=True)


@cli.command("drop-db")
def drop_db():
    click.confirm("This will DROP all tables in Postgres. Continue?", abort=True)
    sql = "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
    sp.run(
        f'docker exec -i {POSTGRES_CONTAINER_NAME} psql -U {POSTGRES_USER} -d {POSTGRES_DB} -c "{sql}"',
        shell=True,
        check=True,
    )


@cli.command("import-csv")
@click.argument("data_dir", default="../data")
def import_csv(data_dir):
    sp.run(f"uv run python scripts/import_csv.py {data_dir}", shell=True, check=True)


@cli.command("export-csv")
@click.argument("export_path", type=click.Path(file_okay=False, writable=True))
def export_csv(export_path):
    """Export all tables to the specified directory."""
    export_dir = Path(export_path)
    export_dir.mkdir(parents=True, exist_ok=True)

    tables = ["stemming", "motie", "indieners", "details"]
    for table in tables:
        tmp_path = f"/tmp/{table}.csv"
        sp.run(
            f"docker exec -i {POSTGRES_CONTAINER_NAME} "
            f"psql -U {POSTGRES_USER} -d {POSTGRES_DB} "
            f"-c \"\\COPY {table} TO '{tmp_path}' CSV HEADER\"",
            shell=True,
            check=True,
        )
        sp.run(
            f"docker cp {POSTGRES_CONTAINER_NAME}:{tmp_path} {export_dir}/{table}.csv",
            shell=True,
            check=True,
        )
    click.echo(f"Exported tables to {export_dir.resolve()}")


@cli.command()
def psql():
    sp.run(
        f"docker exec -it {POSTGRES_CONTAINER_NAME} psql -U {POSTGRES_USER} -d {POSTGRES_DB}",
        shell=True,
    )


if __name__ == "__main__":
    cli()
