import pandas as pd
from typing import Optional, Dict
from .utils import _latest_month, _prev_quarter_month, _window_bounds
from .core_spines import (
    _mrr_spine,
    _logos_spine,
    _cac_spine,
    _revenue_and_months_new_logos_spine,
)


# -------------- Top Row KPIs --------------#
def top_row_kpis(
    conn,
    product_id: Optional[str] = None,
    country: Optional[str] = None,
    billing_cycle: Optional[str] = None,
    time_range: str = "Last 12M",
    end_month: Optional[str] = None,
) -> Dict[str, float]:
    """Calculate top row KPIs for revenue analytics dashboard."""

    # If end_month is not provided, use the latest month from the data
    if end_month is None:
        end_month = _latest_month(
            _mrr_spine(conn, product_id, country, billing_cycle, None, None)["month"]
        )

    # Get start and end months based on time_range
    start_month, end_month = _window_bounds(conn, end_month, time_range)

    # Get MRR spine
    mrr = _mrr_spine(conn, product_id, country, billing_cycle, start_month, end_month)

    # Month anchors
    curr_month = end_month
    prev_quarter_month = _prev_quarter_month(curr_month) if curr_month else None
    prev_month = str(pd.Period(curr_month, freq="M") - 1) if curr_month else None

    # MRR for the period
    total_mrr = mrr["mrr"].sum() if not mrr.empty else 0.0

    # ARR (current month MRR * 12)
    curr_rev = mrr[mrr["month"] == curr_month]["mrr"].sum() if curr_month else 0.0
    arr = curr_rev * 12

    # New and churned logos in the period
    logos = _logos_spine(
        conn, product_id, country, billing_cycle, start_month, end_month
    )
    new_logos = logos["new_logos"].sum() if not logos.empty else 0.0
    churned_logos = logos["churned_logos"].sum() if not logos.empty else 0.0

    # CAC
    cac = _cac_spine(conn, start_month=start_month, end_month=end_month)
    total_cac = cac["cac"].sum() if not cac.empty else 0.0  # total marketing spend
    avg_cac = (total_cac / new_logos) if new_logos > 0 else 0.0  # cac per new logo
    revenue_new_logos = _revenue_and_months_new_logos_spine(
        conn, start_month=start_month, end_month=end_month
    )
    total_revenue_from_new_logos = (
        revenue_new_logos["revenue_in_period"].sum()
        if not revenue_new_logos.empty
        else 0.0
    )
    total_active_months_from_new_logos = (
        revenue_new_logos["months_in_period"].sum()
        if not revenue_new_logos.empty
        else 0.0
    )
    avg_revenue_per_new_logo = (
        (total_revenue_from_new_logos / total_active_months_from_new_logos)
        if total_active_months_from_new_logos > 0
        else 0.0
    )
    # CAC Payback Period (in months)
    cac_payback = (
        avg_cac / (avg_revenue_per_new_logo * 0.6)  # assuming 60% gross margin
        if avg_revenue_per_new_logo > 0
        else 0.0
    )

    return {
        "mrr": total_mrr,
        "arr": arr,
        "new_logos": new_logos,
        "churned_logos": churned_logos,
        "cac_payback": cac_payback,
        # For debugging / display purposes --Remove later
        "total_cac": total_cac,
        "avg_cac": avg_cac,  # CAC per new logo
        "total_revenue_from_new_logos": total_revenue_from_new_logos,
        "total_active_months_from_new_logos": total_active_months_from_new_logos,
        "avg_revenue_per_new_logo": avg_revenue_per_new_logo,
    }
