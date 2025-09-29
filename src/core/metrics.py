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


def _costs_spine(conn, start_month=None, end_month=None) -> pd.DataFrame:
    """Get the monthly costs spine (COGS + OpEx)."""

    df = _read(conn, q.costs_by_month_sql(start_month, end_month))
    df["month"] = df["month"].astype(str)
    for col in ["cogs", "opex"]:
        df[col] = df[col].astype(float).fillna(0.0)
    return df


def _burn_and_cash_spine(conn, month: str) -> pd.DataFrame:
    """Get the burn and cash balance for a specific month."""

    df = _read(conn, q.burn_and_cash_sql(month))
    df["net_monthly_burn"] = df["net_monthly_burn"].astype(float).fillna(0.0)
    df["ending_cash_balance"] = df["ending_cash_balance"].astype(float).fillna(0.0)
    return df


def _window_bounds(end_month: str | None, time_range: str):
    if not end_month:
        return None, None

    end_dt = pd.to_datetime(end_month)

    if time_range == "Last 12M":
        start_dt = end_dt - pd.DateOffset(months=11)
    elif time_range == "YTD":
        start_dt = pd.to_datetime(f"{end_dt.year}-01")
    elif time_range == "QTD":
        q_start = ((end_dt.month - 1) // 3) * 3 + 1
        start_dt = pd.to_datetime(f"{end_dt.year}-{q_start:02d}")
    else:
        start_dt = None

    start_date_for_query = (start_dt - pd.DateOffset(months=1)) if start_dt else None

    to_str = lambda dt: dt.strftime("%Y-%m") if dt else None

    return to_str(start_date_for_query), to_str(end_dt)


# -------------- KPI Block --------------#
def exec_overview_kpis(
    conn,
    product_id: Optional[str] = None,
    country: Optional[str] = None,
    time_range: str = "Last 12M",
    end_month: Optional[str] = None,
) -> Dict[str, float]:
    """Calculate executive overview KPIs."""

    # If end_month is not provided, use the latest month from the data
    if end_month is None:
        end_month = _latest_month(
            _mrr_spine(conn, product_id, country, None, None)["month"]
        )
    # Get start and end months based on time_range
    start_month, end_month = _window_bounds(end_month, time_range)

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

    # Costs (COGS + OpEx) for latest month
    costs = _costs_spine(conn, start_month, end_month)
    current_month_costs = costs[costs["month"] == curr_month]
    cogs = current_month_costs["cogs"].sum() if not current_month_costs.empty else 0.0
    opex = current_month_costs["opex"].sum() if not current_month_costs.empty else 0.0

    # Margins
    gross_margin = (curr_rev - cogs) / curr_rev if curr_rev > 0 else 0.0
    op_margin = (curr_rev - cogs - opex) / curr_rev if curr_rev > 0 else 0.0

    # Burn and Burn Multiple
    net_new_arr = max((flows["curr_mrr"].sum() - starting_mrr) * 12.0, 0.0)

    if curr_month:
        cash_and_burn = _burn_and_cash_spine(conn, curr_month)
    else:
        cash_and_burn = None

    net_monthly_burn = (
        cash_and_burn["net_monthly_burn"].iloc[0]
        if cash_and_burn is not None and not cash_and_burn.empty
        else 0.0
    )
    ending_cash_balance = (
        cash_and_burn["ending_cash_balance"].iloc[0]
        if cash_and_burn is not None and not cash_and_burn.empty
        else 0.0
    )

    if net_monthly_burn <= 0:
        burn_multiple = 0.0
        runway_months = np.inf
    else:
        burn_multiple = net_monthly_burn / net_new_arr if net_new_arr > 0 else np.inf
        runway_months = (
            ending_cash_balance / net_monthly_burn if ending_cash_balance > 0 else 0.0
        )
    runway_months = 9999.0 if np.isfinite(runway_months) else float(runway_months)

    return dict(
        arr=float(arr),
        arr_growth=float(arr_growth),
        nrr=float(nrr),
        grr=float(grr),
        gross_margin=float(gross_margin),
        op_margin=float(op_margin),
        burn_multiple=float(burn_multiple),
        runway_months=float(runway_months),
        net_monthly_burn=float(net_monthly_burn),
        ending_cash_balance=float(ending_cash_balance),
    )


# -------------- ARR Bridge (monthly) --------------#
def arr_bridge(
    conn, product_id=None, country=None, time_range="Last 12M", end_month=None
):
    """Calculate the ARR bridge components on a monthly basis."""

    # If end_month is not provided, use the latest month from the data
    if end_month is None:
        end_month = _latest_month(
            _mrr_spine(conn, product_id, country, None, None)["month"]
        )
    # Get start and end months based on time_range
    start_month, end_month = _window_bounds(end_month, time_range)

    prev_month = str(pd.Period(end_month, freq="M") - 1) if end_month else None

    # Get MRR spine
    mrr = _mrr_spine(conn, product_id, country, start_month, end_month)
    if mrr.empty:
        return pd.DataFrame(
            columns=[
                "month",
                "starting_mrr",
                "churn",
                "contraction",
                "expansion",
                "new_business",
                "ending_mrr",
            ]
        )

    # Get per customer deltas
    curr = mrr[mrr["month"] == end_month].copy()
    prev = mrr[mrr["month"] == prev_month][["customer_id", "mrr"]].rename(
        columns={"mrr": "prev_mrr"}
    )
    flows = curr.merge(prev, on="customer_id", how="outer").fillna(0.0)

    starting_mrr = flows["prev_mrr"].sum()
    ending_mrr = flows["mrr"].sum()
    new_mrr = flows[(flows["prev_mrr"] == 0) & (flows["mrr"] > 0)]["mrr"].sum()
    expansion = ((flows["mrr"] > flows["prev_mrr"]) & (flows["prev_mrr"] > 0)) * (
        flows["mrr"] - flows["prev_mrr"]
    )
    contraction = ((flows["mrr"] < flows["prev_mrr"]) & (flows["mrr"] > 0)) * (
        flows["prev_mrr"] - flows["mrr"]
    )
    churn = ((flows["prev_mrr"] > 0) & (flows["mrr"] == 0)) * flows["prev_mrr"]

    bridge = pd.DataFrame(
        {
            "step": [
                "Starting ARR",
                "New",
                "Expansion",
                "Contraction",
                "Churn",
                "Ending ARR",
            ],
            "value": [
                starting_mrr * 12,
                new_mrr * 12,
                expansion.sum() * 12,
                -contraction.sum() * 12,
                -churn.sum() * 12,
                ending_mrr * 12,
            ],
            "type": [
                "absolute",
                "relative",
                "relative",
                "relative",
                "relative",
                "total",
            ],
        }
    )
    return bridge
