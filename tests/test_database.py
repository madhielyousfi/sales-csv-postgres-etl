import os
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from sales_etl.pipeline import extract, load, transform

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.integration


@pytest.fixture
def database():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    schema = "etl_test_" + uuid4().hex
    with psycopg.connect(url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    isolated = make_conninfo(url, options=f"-c search_path={schema}")
    try:
        with psycopg.connect(isolated) as connection:
            connection.execute((ROOT / "sql/schema.sql").read_text())
        yield isolated
    finally:
        with psycopg.connect(url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def sample():
    return transform(extract(ROOT / "data/messy_sales.csv")).valid


def test_load_and_rerun(database):
    records = sample()
    assert load(records, database) == (4, 0)
    assert load(records, database) == (0, 4)
    with psycopg.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*), SUM(total_amount) FROM sales").fetchone() == (4, Decimal("49.10"))


@pytest.mark.parametrize("changes", [
    {"quantity": 0}, {"product": " "}, {"unit_price": Decimal("-1")},
    {"total_amount": Decimal("999")}, {"sale_id": -1}, {"customer_name": None},
])
def test_constraints_and_atomic_rollback(database, changes):
    good = sample()[0]
    bad = {**good, "sale_id": 100, **changes}
    with pytest.raises(psycopg.Error):
        load([good, bad], database)
    with psycopg.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM sales").fetchone()[0] == 0
