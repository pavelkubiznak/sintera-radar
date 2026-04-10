# Sintera Radar

B2B acquisition intelligence system for recruitment agencies. Scrapes job listings from jobs.cz, matches them with HR decision makers from LinkedIn, and provides actionable recommendations for outreach.

## Features

- **Jobs.cz Scraper** — automated scraping across all 14 Czech regions
- **Decision Makers DB** — LinkedIn Recruiter CSV import, per-region management
- **Radar** — intelligent matching of companies hiring with identified HR contacts
- **Signal scoring** — prioritizes companies by hiring urgency (long-running positions, repeated listings, team expansion)
- **Batch Messages** — pre-written personalized outreach messages per region
- **Outreach CRM** — track contacted companies, status updates, response pipeline
- **Dashboard** — key metrics, surge alerts, weekly recommendations

## Quick Start

```bash
pip install -r requirements.txt
python3 app.py
```

Open http://localhost:5001

## Setup for Partners

1. Clone this repo
2. `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill in your email credentials (only needed for scheduled email reports)
4. Run `python3 app.py`
5. The database starts empty — use the Scraper page to fetch positions, and Decision Makers page to upload LinkedIn CSVs

## Data

The SQLite database (`data/jobs.db`) is not included in the repo. It gets created automatically on first run. To share your existing data with partners, send them the `jobs.db` file separately.

## Architecture

- **Flask** web app with SQLite backend (WAL mode)
- **Jinja2** templates with shared sidebar layout
- Background threading for scraper jobs
- Company name normalization for cross-platform matching (jobs.cz ↔ LinkedIn)
