import click
from pathlib import Path
from scrape.main import run as run_scraper
from datetime import datetime


DEFAULT_OUTPUT_DIR = Path("../data")


@click.group()
def cli():
    """Manage data scraper for stemmingsuitslagen."""
    pass


@cli.command()
@click.argument("from_date", type=str)
@click.argument("to_date", type=str, required=False)
@click.argument("output_dir", type=str, default=DEFAULT_OUTPUT_DIR)
def run(from_date, to_date, output_dir):
    """Scrape Tweede Kamer motions from BEGIN_PAGE to END_PAGE."""
    from_date = datetime.strptime(from_date, "%Y-%m-%d").date()
    to_date = datetime.strptime(to_date, "%Y-%m-%d").date() if to_date else None
    run_scraper(from_date, to_date, output_dir)


if __name__ == "__main__":
    cli()
