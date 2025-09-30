import pandas as pd
from typing import Optional
from ..queries.queries_mrr import data_bounds_sql


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


def _data_bounds(conn) -> tuple[pd.Period, pd.Period]:
    """Get the min and max month available in the data."""

    df = _read(conn, data_bounds_sql())

    min_month = df["min_month"].iloc[0]
    max_month = df["max_month"].iloc[0]

    return pd.Period(min_month, freq="M"), pd.Period(max_month, freq="M")


def _window_bounds(conn, end_month: str | None, time_range: str):
    """Get the start and end month strings (YYYY-MM) for a given time range ending at end_month."""
    if not end_month:
        return None, None

    end_dt = pd.Period(end_month, freq="M")

    if time_range == "Last 12M":
        start_dt = end_dt - 11
    elif time_range == "YTD":
        start_dt = pd.Period(f"{end_dt.year}-01", freq="M")
    elif time_range == "QTD":
        q_start = ((end_dt.month - 1) // 3) * 3 + 1
        start_dt = pd.Period(f"{end_dt.year}-{q_start:02d}", freq="M")
    else:
        start_dt = None

    start_date_for_query = (start_dt - 1) if start_dt else None

    # Check of date bounds in the database
    db_min_month, db_max_month = _data_bounds(conn)
    if start_date_for_query and db_min_month and start_date_for_query < db_min_month:
        start_date_for_query = db_min_month
    if end_dt and db_max_month and end_dt > db_max_month:
        end_dt = db_max_month

    to_str = lambda dt: str(dt) if dt else None

    return to_str(start_date_for_query), to_str(end_dt)
