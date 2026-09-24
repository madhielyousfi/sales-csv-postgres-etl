# Sales CSV → PostgreSQL ETL

A small Python project for learning **Extract, Transform, Load**. Pandas cleans a messy CSV; Python validates records and calculates exact monetary totals; Psycopg inserts valid sales into PostgreSQL in one transaction.

## 1. Set up

You need Python 3.10 or newer and Docker with Docker Compose. Run all commands from this project directory. The examples use a Linux/macOS shell.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
cp .env.example .env
docker compose up -d --wait
```

PostgreSQL listens on local port **5433**. Compose creates the `sales` database and applies `sql/schema.sql` on the first start of an empty database volume. The example credentials are for this local tutorial. Keep `.env` out of version control.

For an existing PostgreSQL instance, set `DATABASE_URL` in `.env` and apply `sql/schema.sql` to that database using your SQL client. To explicitly reapply the setup to the Compose database:

```bash
docker compose exec -T db psql -U sales -d sales -v ON_ERROR_STOP=1 < sql/schema.sql
```

The setup is safe to rerun but does not migrate an existing table with a different structure.

## 2. Run the pipeline

```bash
python -m sales_etl --input data/messy_sales.csv
```

The first run against an empty table prints:

```text
read=13 rejected=8 deduplicated=1 inserted=4 skipped_existing=0
```

Inspect `output/messy_sales_rejected.csv` for the eight rejected rows and their reasons. The report retains original input values, plus `source_row` (the physical line where the record begins, with the header at line 1). It is regenerated on every run, including a header-only report when there are no rejects.

Run the same command again: `inserted=0 skipped_existing=4`. The primary key and `ON CONFLICT DO NOTHING` make repeated loads safe. A sale ID already in the database is skipped even if the new file has different values; this pipeline does not update existing sales.

## 3. Understand the steps

**Extract:** The standard CSV reader checks structure and retains source lines before building a Pandas DataFrame of strings. This prevents automatic numeric conversion or accidental missing-value guesses. Inputs must be UTF-8, comma-separated, with a header. Missing files, invalid encoding, malformed CSV, inconsistent field counts, and duplicate or missing column names fail the run. Extra columns are ignored. A header-only CSV is allowed; blank data lines are malformed records.

**Transform:** Pandas trims surrounding whitespace and normalizes blank values, `NA`, and `null` (case insensitive). Blank customer names become `Unknown`. Required columns are:

| Column | Validation |
| --- | --- |
| `sale_id` | Integer from 1 to 2,147,483,647 |
| `sale_date` | Valid date written exactly as YYYY-MM-DD, years 0001–9999 |
| `customer_name` | Nonblank text after filling missing names |
| `product` | Required nonblank text |
| `quantity` | Integer from 1 to 2,147,483,647 |
| `unit_price` | Nonnegative plain decimal, at most two fractional digits, maximum 9,999,999,999.99 |

Text cannot contain NUL characters because PostgreSQL text cannot store them. Currency symbols, thousands separators, exponent notation, and signed numeric strings are rejected. `total_amount` is calculated with `Decimal`, not floating point, and must fit `NUMERIC(18,2)` (maximum 9,999,999,999,999,999.99).

Valid identical records are deduplicated after normalization and numeric conversion: `12.5` and `12.50` are equivalent. If one ID has different records within the file, all its records are rejected, including when one version has another validation error. Identical invalid records remain separate rejects so each source row can be corrected. Counts satisfy `read = rejected + deduplicated + inserted + skipped_existing` on successful runs.

**Load:** Parameterized SQL inserts valid records in one transaction. Database constraints reinforce validation and ensure totals match quantity × price. PostgreSQL numeric columns round excess decimal places on direct SQL inserts; the pipeline rejects excess precision before insertion. Any database failure rolls back the batch. Rejected input rows do not prevent valid rows from loading.

The rejection report is written before loading so an unwritable report fails before database changes. The filesystem report and database transaction are separate: a report can exist even if loading fails. File, output, configuration, and database failures exit with status 1; completed processing exits with status 0 even when rows are rejected. Argument errors exit with status 2. Database error logs omit connection details to avoid exposing passwords.

## 4. Query the results

```bash
docker compose exec -T db psql -U sales -d sales < sql/queries.sql
```

The sample inserts sale IDs **1, 2, 3, 10**. Total revenue is **49.10**. Product revenue is Notebook **25.00**, Mouse **20.00**, Pen **3.60**, and Sticker **0.50**. Daily revenue is January 1 **28.60**, January 2 **20.00**, and January 3 **0.50**, all in 2026.

## 5. Run tests

```bash
python -m pytest -q -m 'not integration'
TEST_DATABASE_URL=postgresql://sales:sales_local@localhost:5433/sales python -m pytest -q
```

Without `TEST_DATABASE_URL`, database tests are skipped. Integration tests create a uniquely named schema, test insertion, reruns, constraints, and transaction rollback, then remove only that schema. Use a disposable local database account with permission to create schemas. They do not clear the tutorial's sales table.

The code lives in `sales_etl/pipeline.py`; CLI orchestration is in `sales_etl/__main__.py`. The sample and SQL scripts are in `data/` and `sql/`.

To stop PostgreSQL while preserving data:

```bash
docker compose down
```

This tutorial processes one small file in memory per command. Prices share one currency. Scheduling, refunds, currency conversion, dashboards, and cloud deployment are outside its scope.
