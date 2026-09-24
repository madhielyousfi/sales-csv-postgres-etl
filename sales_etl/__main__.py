import argparse
import logging
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from sales_etl.pipeline import PipelineError, extract, load, transform, write_rejections


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean a sales CSV and load valid records into PostgreSQL")
    parser.add_argument("--input", required=True, type=Path, help="Path to a UTF-8 sales CSV")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    load_dotenv()
    try:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise PipelineError("DATABASE_URL is missing; copy .env.example to .env and configure it")
        frame = extract(args.input)
        result = transform(frame)
        report = Path("output") / f"{args.input.stem}_rejected.csv"
        # Ensure the report is writable before any database changes.
        write_rejections(result.rejected, report)
        inserted, skipped = load(result.valid, database_url)
        logging.info(
            "read=%d rejected=%d deduplicated=%d inserted=%d skipped_existing=%d",
            len(frame), len(result.rejected), result.duplicate_count, inserted, skipped,
        )
        logging.info("Rejected-row report: %s", report)
        return 0
    except PipelineError as exc:
        logging.error("%s", exc)
        return 1
    except psycopg.Error as exc:
        # Avoid logging connection strings, credentials, or rejected data.
        logging.error("Database operation failed (%s). Check connection settings and run sql/schema.sql; the load transaction was not committed.", type(exc).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
