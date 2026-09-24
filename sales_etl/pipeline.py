"""Small, independent ETL steps with explicit validation rules."""

import csv
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import psycopg

COLUMNS = ["sale_id", "sale_date", "customer_name", "product", "quantity", "unit_price"]
SALE_COLUMNS = COLUMNS + ["total_amount"]
INT_MAX = 2_147_483_647
PRICE_MAX = Decimal("9999999999.99")
TOTAL_MAX = Decimal("9999999999999999.99")


class PipelineError(Exception):
    """An actionable input or output error."""


@dataclass
class TransformResult:
    valid: list[dict]
    rejected: list[dict]
    duplicate_count: int


def extract(path: Path) -> pd.DataFrame:
    """Read strings with strict CSV structure checks; retain physical start lines."""
    rows = []
    try:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream, strict=True)
            header = next(reader, None)
            if header is None:
                raise PipelineError("CSV is empty; a header is required")
            if len(header) != len(set(header)):
                raise PipelineError("CSV contains duplicate column names")
            missing = set(COLUMNS) - set(header)
            if missing:
                raise PipelineError("Missing required columns: " + ", ".join(sorted(missing)))
            previous_line = reader.line_num
            for values in reader:
                source_row = previous_line + 1
                previous_line = reader.line_num
                if len(values) != len(header):
                    raise PipelineError(f"Malformed CSV at row {source_row}: expected {len(header)} fields, got {len(values)}")
                record = dict(zip(header, values))
                rows.append({**{key: record[key] for key in COLUMNS}, "source_row": source_row})
    except (OSError, UnicodeError, csv.Error) as exc:
        raise PipelineError(f"Cannot read CSV '{path}': {exc}") from exc
    return pd.DataFrame(rows, columns=COLUMNS + ["source_row"])


def normalize(value: str) -> str:
    value = value.strip()
    return "" if value.lower() in {"na", "null", ""} else value


def positive_integer(value: str, field: str) -> int:
    if not re.fullmatch(r"[0-9]+", value) or len(value.lstrip("0")) > 10:
        raise ValueError(f"{field} must be a positive integer")
    result = int(value.lstrip("0") or "0")
    if not 1 <= result <= INT_MAX:
        raise ValueError(f"{field} must be between 1 and {INT_MAX}")
    return result


def transform(frame: pd.DataFrame) -> TransformResult:
    cleaned = frame.copy()
    for column in COLUMNS:
        cleaned[column] = cleaned[column].map(normalize)
    cleaned["customer_name"] = cleaned["customer_name"].replace("", "Unknown")
    candidates, rejected = [], []
    for raw, row in zip(frame.to_dict("records"), cleaned.to_dict("records")):
        errors = []
        sale = {}
        for field in ("sale_id", "quantity"):
            try:
                sale[field] = positive_integer(row[field], field)
            except ValueError as exc:
                errors.append(str(exc))
        try:
            if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", row["sale_date"]):
                raise ValueError
            sale["sale_date"] = date.fromisoformat(row["sale_date"])
        except ValueError:
            errors.append("sale_date must be a valid YYYY-MM-DD date")
        for field in ("customer_name", "product"):
            sale[field] = row[field]
            if not row[field] or "\x00" in row[field]:
                errors.append(f"{field} must be nonblank text without NUL characters")
        price_text = row["unit_price"]
        if not re.fullmatch(r"[0-9]+(?:\.[0-9]{1,2})?", price_text):
            errors.append("unit_price must be a nonnegative decimal with at most two decimal places")
        else:
            price = Decimal(price_text)
            if price > PRICE_MAX:
                errors.append("unit_price exceeds NUMERIC(12,2) bounds")
            else:
                sale["unit_price"] = price.quantize(Decimal("0.01"))
                if "quantity" in sale:
                    total = sale["quantity"] * sale["unit_price"]
                    if total > TOTAL_MAX:
                        errors.append("total_amount exceeds NUMERIC(18,2) bounds")
                    else:
                        sale["total_amount"] = total
        candidates.append((raw, row, sale, errors))

    # Compare canonical values, even when another field is invalid. This prevents
    # accepting one version of an ID whose other version was rejected.
    signatures = {}
    for raw, row, sale, errors in candidates:
        if "sale_id" in sale:
            signature = tuple(sale.get(key, row[key]) for key in COLUMNS)
            signatures.setdefault(sale["sale_id"], set()).add(signature)
    valid, seen, duplicates = [], set(), 0
    for raw, row, sale, errors in candidates:
        if len(signatures.get(sale.get("sale_id"), set())) > 1:
            errors.append("conflicting records share this sale_id")
        if errors:
            rejected.append({**raw, "rejection_reason": "; ".join(errors)})
        elif sale["sale_id"] in seen:
            duplicates += 1
        else:
            seen.add(sale["sale_id"])
            valid.append(sale)
    return TransformResult(valid, rejected, duplicates)


def write_rejections(records: list[dict], path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(records, columns=COLUMNS + ["source_row", "rejection_reason"]).to_csv(path, index=False)
    except (OSError, UnicodeError) as exc:
        raise PipelineError(f"Cannot write rejection report '{path}': {exc}") from exc


def load(records: list[dict], database_url: str) -> tuple[int, int]:
    """Connection context commits on success and rolls back on any exception."""
    inserted = 0
    with psycopg.connect(database_url, connect_timeout=10) as connection:
        with connection.cursor() as cursor:
            for record in records:
                cursor.execute(
                    "INSERT INTO sales (sale_id, sale_date, customer_name, product, quantity, unit_price, total_amount) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (sale_id) DO NOTHING",
                    tuple(record[key] for key in SALE_COLUMNS),
                )
                inserted += cursor.rowcount
    return inserted, len(records) - inserted
