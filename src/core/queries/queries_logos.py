from typing import Optional, Dict, Tuple
from .query_helpers import _filters
import pandas as pd


def logos_by_month_sql(
    product_id: Optional[str] = None,
    country: Optional[str] = None,
    billing_cycle: Optional[str] = None,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
) -> Tuple[str, Dict]:
    """Generate SQL to get monthly new and churned logos with optional filters."""

    orig_start = start_month
    orig_end = end_month

    # start_month - 1 month and end_month + 1 month to capture new/churned logos correctly
    if start_month:
        start_month = str(pd.Period(start_month, freq="M") - 1)
    if end_month:
        end_month = str(pd.Period(end_month, freq="M") + 1)

    where, params = _filters(product_id, country, billing_cycle, start_month, end_month)
    if orig_start:
        params["orig_start"] = orig_start
    if orig_end:
        params["orig_end"] = orig_end

    sql = f"""
    WITH base AS (
        SELECT
            DISTINCT DATE_TRUNC('month', fr.date_id::DATE) AS month,
            fr.customer_id,
            fr.product_id,
            fr.billing_cycle,
            dc.country
        FROM core.fact_subscription_revenue fr
        JOIN core.dim_date dd ON dd.date = fr.date_id::DATE
        LEFT JOIN core.dim_customer dc ON dc.customer_id = fr.customer_id
        LEFT JOIN core.dim_product dp ON dp.product_id = fr.product_id
        {where}
    ),
    gm AS (
        -- last observable month
        SELECT MAX(month) AS max_month FROM base
    ),
    labeled AS (
        SELECT
            c.month,
            CASE WHEN p.customer_id IS NULL THEN 1 ELSE 0 END AS new_logo,
            CASE 
                WHEN c.month = gm.max_month THEN 0
                WHEN n.customer_id IS NULL THEN 1 
                ELSE 0 
            END AS churned_logo
        FROM base c
        LEFT JOIN base p
            ON p.customer_id = c.customer_id
            AND p.month = c.month - INTERVAL '1 month'
        LEFT JOIN base n
            ON n.customer_id = c.customer_id
            AND n.month = c.month + INTERVAL '1 month'
        CROSS JOIN gm
    )
    SELECT TO_CHAR(month, 'YYYY-MM') AS month,
        SUM(new_logo) AS new_logos,
        SUM(churned_logo) AS churned_logos
    FROM labeled
    -- bring output back to the original visible window
    WHERE (%(orig_start)s IS NULL OR TO_CHAR(month, 'YYYY-MM') >= %(orig_start)s)
      AND (%(orig_end)s   IS NULL OR TO_CHAR(month, 'YYYY-MM') <= %(orig_end)s)
    GROUP BY 1
    ORDER BY 1;
    """
    return sql, params
