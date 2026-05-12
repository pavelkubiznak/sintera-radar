"""Nightly HR Hunter auto-enrichment.

Run via cron after the scraper:
  0 4 * * *  cd /opt/sintera-radar && python3 cron_auto_enrich.py >> logs/enrich.log 2>&1

Picks 30 problematic firms (3+ active positions, no LinkedIn DM,
not scraped in last 7 days) and enriches them sequentially.
"""
import sys, os, json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from enrichment import nightly_auto_enrich


def main():
    limit = 30
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            pass

    start = datetime.now()
    print("=" * 60)
    print(f"[{start.isoformat(timespec='seconds')}] Starting auto-enrich")
    print(f"Limit: {limit}")

    stats = nightly_auto_enrich(limit=limit)

    duration = (datetime.now() - start).total_seconds()
    print(f"\nResults:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print(f"Duration: {duration:.1f}s")
    print(f"Done at {datetime.now().isoformat(timespec='seconds')}")


if __name__ == "__main__":
    main()
