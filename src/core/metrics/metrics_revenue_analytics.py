import pandas as pd
from typing import Optional, Dict
from .utils import _latest_month, _prev_quarter_month, _window_bounds
from .core_spines import _mrr_spine, _logos_spine


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

    print(mrr.head())

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

    return {
        "mrr": total_mrr,
        "arr": arr,
        "new_logos": new_logos,
        "churned_logos": churned_logos,
        "cac_payback": 12.0,
    }
