# tweede-kamer stemmingsuitslagen scraper

A Python scraper for Tweede Kamer motions and voting results from [here](https://www.tweedekamer.nl/kamerstukken/stemmingsuitslagen).

It extracts motion titles, motion type, text (from HTML or DOCX), vote results, and other metadata, storing everything in a CSV-friendly format. Each scraped voting session is saved in its own folder, named after the `stemming_id`.

**NOTE:** Voting details may appear on the website with a slight delay.

**NOTE:** Wetsvoorstellen are skipped for now, as they have many steps and I still have to figure out how they work.

## Features

- Scrapes motion title and type (`Motie`, `Brief commissie`, etc.)
- Extracts text from the HTML page or fallback DOCX downloads
- Extracts voting results, including fracties, zetels, and voor/tegen

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/tweede-kamer-scraper.git
cd tweede-kamer-scraper/01_scrape
```

2. Install dependencies using uv:

```bash
uv install
```

## Usage

To see CLI options and help:

```bash
uv run python -m cli --help
```

Example commands:

```bash
uv run python -m cli run 2025-01-01             # Scrape from Jan 1st, 2025 onwards
uv run python -m cli run 2025-01-01 2025-01-31  # Scrape the month of Januari, 2025
```

This will scrape the specified pages and extract all relevant motion data.
