import pandas as pd
from .utils import _read
from ..queries.queries_mrr import (
    monthly_customer_mrr_sql,
    costs_by_month_sql,
    burn_and_cash_sql,
)
from ..queries.queries_logos import logos_by_month_sql
from ..queries.queries_cac import (
    cac_by_month_sql,
    total_revenue_and_months_new_logos_sql,
)


# -------------- Core Spines --------------#
def _mrr_spine(
    conn,
    product_id=None,
    country=None,
    billing_cycle=None,
    start_month=None,
    end_month=None,
) -> pd.DataFrame:
    """Get the monthly MRR spine with optional filters."""

    if product_id == "All":
        product_id = None
    if country == "All":
        country = None

    df = _read(
        conn,
        monthly_customer_mrr_sql(
            product_id, country, billing_cycle, start_month, end_month
        ),
    )
    df["month"] = df["month"].astype(str)
    df["mrr"] = df["mrr"].astype(float).fillna(0.0)
    return df


def _costs_spine(conn, start_month=None, end_month=None) -> pd.DataFrame:
    """Get the monthly costs spine (COGS + OpEx)."""

    df = _read(conn, costs_by_month_sql(start_month, end_month))
    df["month"] = df["month"].astype(str)
    for col in ["cogs", "opex"]:
        df[col] = df[col].astype(float).fillna(0.0)
    return df


def _burn_and_cash_spine(conn, month: str) -> pd.DataFrame:
    """Get the burn and cash balance for a specific month."""

    df = _read(conn, burn_and_cash_sql(month))
    df["net_monthly_burn"] = df["net_monthly_burn"].astype(float).fillna(0.0)
    df["ending_cash_balance"] = df["ending_cash_balance"].astype(float).fillna(0.0)
    return df


# -------------- Logos Spine --------------#


def _logos_spine(
    conn,
    product_id=None,
    country=None,
    billing_cycle=None,
    start_month=None,
    end_month=None,
) -> pd.DataFrame:
    """Get the monthly logos spine with optional filters."""

    if product_id == "All":
        product_id = None
    if country == "All":
        country = None
    if billing_cycle == "All":
        billing_cycle = None

    df = _read(
        conn,
        logos_by_month_sql(product_id, country, billing_cycle, start_month, end_month),
    )
    df["month"] = df["month"].astype(str)
    df["new_logos"] = df["new_logos"].astype(int).fillna(0)
    df["churned_logos"] = df["churned_logos"].astype(int).fillna(0)
    return df


# -------------- CAC spines --------------#


def _cac_spine(
    conn,
    start_month=None,
    end_month=None,
) -> pd.DataFrame:
    """Get the monthly CAC spine with optional filters."""

    df = _read(conn, cac_by_month_sql(start_month, end_month))
    df["month"] = df["month"].astype(str)
    df["cac"] = df["cac"].astype(float).fillna(0.0)
    return df


def _revenue_and_months_new_logos_spine(
    conn,
    start_month=None,
    end_month=None,
) -> pd.DataFrame:
    """Get the monthly revenue from new logos spine with optional filters."""

    df = _read(conn, total_revenue_and_months_new_logos_sql(start_month, end_month))
    df["revenue_in_period"] = (
        df["revenue_in_period"].astype(float).fillna(0.0)
    )
    df["months_in_period"] = (
        df["months_in_period"].astype(int).fillna(0)
    )
    return df
