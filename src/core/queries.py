from typing import Optional, Dict, Tuple


def _filters(
    product_id: Optional[str],
    country: Optional[str],
    start_month: Optional[str],
    end_month: Optional[str],
) -> Tuple[str, Dict[str, str]]:
    """Generate SQL WHERE clause and params based on optional filters."""
    parts, params = [], {}
    if product_id:
        parts.append("dp.product_id = %(product_id)s")
        params["product_id"] = product_id
    if country:
        parts.append("dc.country = %(country)s")
        params["country"] = country
    if start_month:
        parts.append("to_char(date_trunc('month', dd.date), 'YYYY-MM') >= %(start_m)s")
        params["start_m"] = start_month
    if end_month:
        parts.append("to_char(date_trunc('month', dd.date), 'YYYY-MM') <= %(end_m)s")
        params["end_m"] = end_month
    where = (" WHERE " + " AND ".join(parts)) if parts else ""
    return where, params


def monthly_customer_mrr_sql(
    product_id: Optional[str] = None,
    country: Optional[str] = None,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
) -> Tuple[str, Dict]:
    where, params = _filters(product_id, country, start_month, end_month)
    sql = f"""
    WITH monthly AS (
        SELECT
            fr.customer_id, 
            TO_CHAR(DATE_TRUNC('month', fr.snapshot_month), 'YYYY-MM') AS month, 
            SUM(fr.mrr_value) AS mrr
        FROM core.fact_subscription_snapshot_monthly fr
        JOIN core.dim_date dd ON dd.date_id = fr.snapshot_month
        JOIN core.dim_customer dc ON dc.customer_id = fr.customer_id
        JOIN core.dim_product dp ON dp.product_id = fr.product_id
        {where}
        GROUP BY 1, 2
        ),
    anchors AS (
        SELECT customer_id, MIN(month) AS first_paid_month
        FROM monthly
        WHERE mrr > 0
        GROUP BY 1
    )
    SELECT m.customer_id, m.month, m.mrr::NUMERIC AS mrr, a.first_paid_month
        FROM monthly m
        LEFT JOIN anchors a
        ON m.customer_id = a.customer_id;
    """
    return sql, params


def costs_by_month_sql(
    start_month: Optional[str] = None, end_month: Optional[str] = None
) -> Tuple[str, Dict]:
    """Generate SQL to get monthly costs (COGS + OpEx) with optional date bounds."""

    params: Dict = {}
    bounds = []

    if start_month:
        bounds.append("TO_CHAR(DATE_TRUNC('month', date_id), 'YYYY-MM') >= %(start_m)s")
        params["start_m"] = start_month
    if end_month:
        bounds.append("TO_CHAR(DATE_TRUNC('month', date_id), 'YYYY-MM') <= %(end_m)s")
        params["end_m"] = end_month
    bound = "WHERE " + " AND ".join(bounds) if bounds else ""

    sql = f"""
    WITH cogs_cloud AS (
        SELECT TO_CHAR(DATE_TRUNC('month', date_id), 'YYYY-MM') AS month,
            SUM(amount_lcy) AS amount
        FROM core.fact_cloud_cost
        {bound}
        GROUP BY 1
    ),
        cogs_payment AS (
            SELECT TO_CHAR(DATE_TRUNC('month', date_id), 'YYYY-MM') AS month,
                SUM(amount_lcy) AS amount
            FROM core.fact_payment_processing_cost
            {bound}
            GROUP BY 1
    ), 
        opex_others AS (
            SELECT TO_CHAR(DATE_TRUNC('month', date_id), 'YYYY-MM') AS month,
                SUM(amount_lcy) AS amount
            FROM core.fact_other_expenses
            {bound}
            GROUP BY 1
    ),
        opex_marketing AS (
            SELECT TO_CHAR(DATE_TRUNC('month', date_id), 'YYYY-MM') AS month,
                SUM(amount_lcy) AS amount
            FROM core.fact_marketing_spend
            {bound}
            GROUP BY 1
    )
    SELECT m.month,
        COALESCE(cc.amount, 0) + COALESCE(pp.amount, 0) AS cogs,
        COALESCE(ox.amount, 0) + COALESCE(mkt.amount, 0) AS opex
    FROM (
        SELECT DISTINCT month FROM (
            SELECT month FROM cogs_cloud
            UNION
            SELECT month FROM cogs_payment
            UNION
            SELECT month FROM opex_others
            UNION
            SELECT month FROM opex_marketing
        ) u
    ) m
    LEFT JOIN cogs_cloud cc ON cc.month = m.month
    LEFT JOIN cogs_payment pp ON pp.month = m.month
    LEFT JOIN opex_others ox ON ox.month = m.month
    LEFT JOIN opex_marketing mkt ON mkt.month = m.month
    ORDER BY m.month;
    """

    return sql, params
