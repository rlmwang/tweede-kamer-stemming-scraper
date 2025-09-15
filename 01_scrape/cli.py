from datetime import datetime
from pathlib import Path

import click

from scrape import main as scraper

DEFAULT_OUTPUT_DIR = Path("../data")


@click.group()
def cli():
    """Manage data scraper for stemmingsuitslagen."""
    pass


@cli.command()
@click.argument("from_date", type=str)
@click.argument("to_date", type=str, required=False)
@click.argument("output_dir", type=str, default=DEFAULT_OUTPUT_DIR)
@click.option("--full_refresh", is_flag=True)
@click.option("--select", type=str)
def run(from_date, to_date, output_dir, full_refresh, select):
    """Scrape Tweede Kamer motions from BEGIN_PAGE to END_PAGE."""
    from_date = datetime.strptime(from_date, "%Y-%m-%d").date()
    to_date = datetime.strptime(to_date, "%Y-%m-%d").date() if to_date else None
    scraper.run(
        output_dir=output_dir,
        from_date=from_date,
        to_date=to_date,
        full_refresh=full_refresh,
        select=select,
    )


@cli.command()
@click.argument("output_dir", type=str, default=DEFAULT_OUTPUT_DIR)
def rebuild_progress(output_dir):
    scraper.rebuild_progress(output_dir)


if __name__ == "__main__":
    cli()
