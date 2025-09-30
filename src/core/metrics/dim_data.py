import pandas as pd
from typing import Optional, Dict
from ..queries.queries_dim import (
    get_all_products_sql,
    get_all_countries_sql,
    get_all_months_sql,
)


def get_all_products(conn) -> pd.DataFrame:
    """Get all products from the dimension table."""
    sql, params = get_all_products_sql()
    return pd.read_sql(sql, conn, params=params)


def get_all_countries(conn) -> pd.DataFrame:
    """Get all countries from the dimension table."""
    sql, params = get_all_countries_sql()
    return pd.read_sql(sql, conn, params=params)


def get_all_months(conn) -> pd.DataFrame:
    """Get all months from the dimension table."""
    sql, params = get_all_months_sql()
    return pd.read_sql(sql, conn, params=params)
