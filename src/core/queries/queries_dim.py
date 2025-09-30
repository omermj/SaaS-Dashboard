from typing import Optional, Dict, Tuple


def get_all_products_sql() -> Tuple[str, Dict]:
    """Generate SQL to get all product names and IDs."""
    sql = """
    SELECT product_id, product_name
    FROM core.dim_product
    """
    return sql, {}


def get_all_countries_sql() -> Tuple[str, Dict]:
    """Generate SQL to get all countries."""
    sql = """
    SELECT DISTINCT country
    FROM core.dim_customer
    WHERE country IS NOT NULL
    ORDER BY country
    """
    return sql, {}


def get_all_months_sql() -> Tuple[str, Dict]:
    """Generate SQL to get all months in YYYY-MM format."""
    sql = """
    SELECT DISTINCT TO_CHAR(DATE_TRUNC('month', date), 'YYYY-MM') AS month
    FROM core.dim_date
    ORDER BY month DESC
    """
    return sql, {}
