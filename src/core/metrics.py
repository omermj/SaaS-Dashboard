from __future__ import annotations
from typing import Optional, Dict
import numpy as np
import pandas as pd
from . import queries as q


# -------------- Utilities --------------#
def _read(conn, sql_params):
    """Helper to read SQL with params."""
    sql, params = sql_params
    return pd.read_sql(sql, conn, params=params)


def _latest_month(series: pd.Series) -> Optional[str]:
    """Get the latest month from a series of YYYY-MM strings."""
    return series.sort_values().iloc[-1] if not series.empty else None


def _prev_quarter_month(curr_month: str) -> Optional[str]:
    """Get the month string (YYYY-MM) for the month 3 months before the given month."""
    p = pd.Period(curr_month, freq="M") - 3
    return str(p)


# -------------- Core Spines --------------#
def _mrr_spine(
    conn, product_id=None, country=None, start_month=None, end_month=None
) -> pd.DataFrame:
    """Get the monthly MRR spine with optional filters."""

    if product_id == "All":
        product_id = None
    if country == "All":
        country = None

    df = _read(
        conn, q.monthly_customer_mrr_sql(product_id, country, start_month, end_month)
    )
    df["month"] = df["month"].astype(str)
    df["mrr"] = df["mrr"].astype(float).fillna(0.0)
    return df


# -------------- KPI Block --------------#
def exec_overview_kpis(
    conn,
    product_id: Optional[str] = None,
    country: Optional[str] = None,
    time_range: str = "Last 12M",
    end_month: Optional[str] = None,
) -> Dict[str, float]:
    """Calculate executive overview KPIs."""

    # Get start and end months based on time_range
    ## If end_month is not provided, use the latest month from the data
    if end_month is None:
        end_month = _latest_month(
            _mrr_spine(conn, product_id, country, None, None)["month"]
        )
    start_month_dt = None
    end_month_dt = pd.to_datetime(end_month) if end_month else None

    if end_month_dt is not None:
        if time_range == "Last 12M":
            start_month_dt = end_month_dt - pd.DateOffset(months=11)
        elif time_range == "YTD":
            start_month_dt = pd.to_datetime(f"{end_month_dt.year}-01")
        elif time_range == "QTD":
            quarter_start_month = ((end_month_dt.month - 1) // 3) * 3 + 1
            start_month_dt = pd.to_datetime(
                f"{end_month_dt.year}-{quarter_start_month:02d}"
            )
        else:
            start_month_dt = None

    start_month = start_month_dt.strftime("%Y-%m") if start_month_dt else None
    end_month = end_month_dt.strftime("%Y-%m") if end_month_dt else None

    # Get MRR spine
    mrr = _mrr_spine(conn, product_id, country, start_month, end_month)
    if mrr.empty:
        return dict(
            arr=0,
            arr_growth=0,
            nrr=0,
            gross_margin=0,
            op_margin=0,
            burn_multiple=0,
            runway_months=0,
        )

    # Month anchors
    curr_month = _latest_month(mrr["month"])
    prev_quarter_month = _prev_quarter_month(curr_month) if curr_month else None
    prev_month = str(pd.Period(curr_month, freq="M") - 1) if curr_month else None

    # Revenue snapshots (latest month)
    curr_rev = mrr[mrr["month"] == curr_month]["mrr"].sum() if curr_month else 0.0
    prev_q_rev = (
        mrr[mrr["month"] == prev_quarter_month]["mrr"].sum()
        if prev_quarter_month
        else 0.0
    )

    # ARR + growth vs last quarter
    arr = curr_rev * 12
    arr_growth = ((curr_rev - prev_q_rev) / prev_q_rev) if prev_q_rev > 0 else 0.0

    # NRR/GRR from latest month flows
    prev = mrr[mrr["month"] == prev_month][["customer_id", "mrr"]].rename(
        columns={"mrr": "prev_mrr"}
    )
    curr = mrr[mrr["month"] == curr_month][["customer_id", "mrr"]].rename(
        columns={"mrr": "curr_mrr"}
    )
    flows = curr.merge(prev, on="customer_id", how="outer").fillna(0.0)

    starting_mrr = flows["prev_mrr"].sum()
    churn = ((flows["prev_mrr"] > 0) & (flows["curr_mrr"] == 0)) * flows["prev_mrr"]
    contraction = (
        (flows["curr_mrr"] < flows["prev_mrr"]) & (flows["curr_mrr"] > 0)
    ) * (flows["prev_mrr"] - flows["curr_mrr"])
    expansion = ((flows["curr_mrr"] > flows["prev_mrr"]) & (flows["prev_mrr"] > 0)) * (
        flows["curr_mrr"] - flows["prev_mrr"]
    )

    grr = (
        (starting_mrr - churn.sum() - contraction.sum()) / starting_mrr
        if starting_mrr > 0
        else 0.0
    )
    nrr = (
        (starting_mrr - churn.sum() - contraction.sum() + expansion.sum())
        / starting_mrr
        if starting_mrr > 0
        else 0.0
    )

    return dict(
        arr=float(arr), arr_growth=float(arr_growth), nrr=float(nrr), grr=float(grr)
    )
