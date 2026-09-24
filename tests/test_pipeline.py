from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from sales_etl.pipeline import COLUMNS, PipelineError, extract, transform, write_rejections

ROOT = Path(__file__).resolve().parents[1]


def row(**changes):
    return {"sale_id": "1", "sale_date": "2026-01-01", "customer_name": " Alice ",
            "product": " Pen ", "quantity": "3", "unit_price": "0.10", "source_row": 2, **changes}


def run(*rows):
    return transform(pd.DataFrame(rows))


def test_sample():
    frame = extract(ROOT / "data/messy_sales.csv")
    result = transform(frame)
    assert len(frame) == 13
    assert len(result.valid) == 4
    assert len(result.rejected) == 8
    assert result.duplicate_count == 1
    assert sum(r["total_amount"] for r in result.valid) == Decimal("49.10")


@pytest.mark.parametrize("name", ["", "  ", "NA", "null", " NULL "])
def test_cleaning(name):
    sale = run(row(customer_name=name)).valid[0]
    assert sale["customer_name"] == "Unknown"
    assert sale["product"] == "Pen"
    assert sale["sale_date"] == date(2026, 1, 1)
    assert sale["total_amount"] == Decimal("0.30")


@pytest.mark.parametrize("field,value", [
    ("sale_id", ""), ("sale_id", "2147483648"), ("quantity", "0"),
    ("quantity", "1.5"), ("quantity", "-1"), ("sale_date", "2026-02-30"),
    ("sale_date", "2026-1-01"), ("product", "NA"), ("product", "a\x00b"),
    ("unit_price", "NaN"), ("unit_price", "Infinity"), ("unit_price", "1e2"),
    ("unit_price", "-1"), ("unit_price", "1.001"), ("unit_price", "10000000000"),
])
def test_invalid(field, value):
    result = run(row(**{field: value}))
    assert not result.valid
    assert field in result.rejected[0]["rejection_reason"]


def test_numeric_limits():
    assert run(row(quantity="1", unit_price="9999999999.99")).valid
    assert run(row(quantity="2147483647", unit_price="0")).valid
    assert "total_amount" in run(row(quantity="2147483647", unit_price="9999999999.99")).rejected[0]["rejection_reason"]


def test_duplicates_and_conflicts():
    result = run(row(), row(unit_price="0.1", source_row=3))
    assert len(result.valid) == 1 and result.duplicate_count == 1
    for changes in ({"quantity": "4"}, {"quantity": "bad"}):
        result = run(row(), row(source_row=3, **changes))
        assert not result.valid and len(result.rejected) == 2
        assert all("conflicting" in r["rejection_reason"] for r in result.rejected)


@pytest.mark.parametrize("contents", ["", "sale_id,product\n1,Pen\n", ",".join(COLUMNS) + "\n1,2\n", ",".join(COLUMNS) + '\n"unclosed'])
def test_bad_csv(tmp_path, contents):
    path = tmp_path / "bad.csv"
    path.write_text(contents)
    with pytest.raises(PipelineError):
        extract(path)


def test_missing_file(tmp_path):
    with pytest.raises(PipelineError, match="Cannot read CSV"):
        extract(tmp_path / "absent.csv")


def test_extra_columns_and_multiline_source_rows(tmp_path):
    path = tmp_path / "input.csv"
    path.write_text(','.join(COLUMNS) + ',extra\n1,2026-01-01,"Alice\nSmith",Pen,1,2,ignored\n2,2026-01-01,Bob,Pen,1,2,x\n')
    frame = extract(path)
    assert list(frame.source_row) == [2, 4]
    assert "extra" not in frame


def test_report_and_output_failure(tmp_path):
    path = tmp_path / "report.csv"
    write_rejections(run(row(quantity="bad")).rejected, path)
    report = pd.read_csv(path)
    assert report.iloc[0].source_row == 2
    assert report.iloc[0].quantity == "bad"
    with pytest.raises(PipelineError, match="Cannot write"):
        write_rejections([], path / "invalid.csv")


def test_header_only(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text(",".join(COLUMNS) + "\n")
    result = transform(extract(path))
    assert result.valid == result.rejected == []
