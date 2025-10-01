from typing import Optional, Dict, Tuple
import pandas as pd


def _filters(
    product_id: Optional[str],
    country: Optional[str],
    billing_cycle: Optional[str],
    start_month: Optional[str],
    end_month: Optional[str],
) -> Tuple[str, Dict[str, str]]:
    """Generate SQL WHERE clause and params based on optional filters."""
    parts, params = [], {}
    if product_id:
        parts.append("fr.product_id = %(product_id)s")
        params["product_id"] = product_id
    if country:
        parts.append("fr.country = %(country)s")
        params["country"] = country
    if billing_cycle:
        parts.append("fr.billing_cycle = %(billing_cycle)s")
        params["billing_cycle"] = billing_cycle
    if start_month:
        parts.append("fr.month_id >= date_trunc('month', %(start_m)s::date)")
        params["start_m"] = f"{start_month}-01"
    if end_month:
        parts.append("fr.month_id <= date_trunc('month', %(end_m)s::date)")
        params["end_m"] = f"{end_month}-01"
    where = (" WHERE " + " AND ".join(parts)) if parts else ""
    return where, params


def logos_by_month_sql(
    product_id: Optional[str] = None,
    country: Optional[str] = None,
    billing_cycle: Optional[str] = None,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
) -> Tuple[str, Dict]:
    """Generate SQL to get monthly new and churned logos with optional filters."""

    orig_start, orig_end = start_month, end_month

    # start_month - 1 month and end_month + 1 month to capture new/churned logos correctly
    if start_month:
        start_month = str(pd.Period(start_month, freq="M") - 1)
    if end_month:
        end_month = str(pd.Period(end_month, freq="M") + 1)

    where, params = _filters(product_id, country, billing_cycle, start_month, end_month)
    if orig_start:
        params["orig_start"] = f"{orig_start}-01"
    if orig_end:
        params["orig_end"] = f"{orig_end}-01"

    sql = f"""
    WITH base AS (
        SELECT
            DISTINCT fr.month_id AS month,
            fr.customer_id,
            fr.product_id,
            fr.billing_cycle,
            fr.country
        FROM core.fact_subscription_revenue fr
        --LEFT JOIN core.dim_customer dc ON dc.customer_id = fr.customer_id
        --LEFT JOIN core.dim_product dp ON dp.product_id = fr.product_id
        {where}
    ),
    --gm AS (
        -- last observable month
    --    SELECT MAX(month) AS max_month FROM base
    --),
    labeled AS (
        SELECT
            c.month,
            CASE WHEN p.customer_id IS NULL THEN 1 ELSE 0 END AS new_logo,
            CASE 
                WHEN c.month = date_trunc('month', %(orig_end)s::date) THEN 0
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
        --CROSS JOIN gm
    )
    SELECT month,
        SUM(new_logo) AS new_logos,
        SUM(churned_logo) AS churned_logos
    FROM labeled
    -- bring output back to the original visible window
    WHERE (%(orig_start)s IS NULL OR month >= %(orig_start)s)
      AND (%(orig_end)s   IS NULL OR month <= %(orig_end)s)
    GROUP BY 1
    ORDER BY 1;
    """
    return sql, params
