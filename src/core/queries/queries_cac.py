from typing import Optional, Dict, Tuple


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
        parts.append("dp.product_id = %(product_id)s")
        params["product_id"] = product_id
    if country:
        parts.append("dc.country = %(country)s")
        params["country"] = country
    if billing_cycle:
        parts.append("fr.billing_cycle = %(billing_cycle)s")
        params["billing_cycle"] = billing_cycle
    if start_month:
        parts.append(
            "(%(start_m)s IS NULL OR TO_CHAR(DATE_TRUNC('month', dd.date), 'YYYY-MM') >= %(start_m)s)"
        )
        params["start_m"] = start_month
    if end_month:
        parts.append(
            "(%(end_m)s IS NULL OR TO_CHAR(DATE_TRUNC('month', dd.date), 'YYYY-MM') <= %(end_m)s)"
        )
        params["end_m"] = end_month
    where = (" WHERE " + " AND ".join(parts)) if parts else ""
    return where, params


def cac_by_month_sql(
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
) -> Tuple[str, Dict]:
    """Generate SQL to get monthly Customer Acquisition Cost (CAC)."""

    params: Dict = {}
    if start_month:
        params["start_m"] = start_month
    if end_month:
        params["end_m"] = end_month

    sql = """
    SELECT
        TO_CHAR(DATE_TRUNC('month', date_id), 'YYYY-MM') AS month,
        SUM(amount_lcy) AS cac
    FROM core.fact_marketing_spend
    WHERE (%(start_m)s IS NULL OR TO_CHAR(date_id, 'YYYY-MM') >= %(start_m)s)
    AND (%(end_m)s IS NULL OR TO_CHAR(date_id, 'YYYY-MM') <= %(end_m)s)
    GROUP BY month
    ORDER BY month;
    """

    return sql, params


def total_revenue_and_months_new_logos_sql(
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
) -> Tuple[str, Dict]:
    """Generate SQL to get total revenue and months active from new logos within a date range."""

    params: Dict = {}
    if start_month:
        params["start_m"] = start_month
    if end_month:
        params["end_m"] = end_month

    sql = """
    WITH new_logos AS (
        -- new customers: appear in the period and not before start_m
        SELECT fr.customer_id,
               MIN(fr.month_id) AS first_month
        FROM core.fact_subscription_revenue fr
        WHERE (%(start_m)s IS NULL OR fr.month_id >= TO_DATE(%(start_m)s || '-01', 'YYYY-MM-DD'))
          AND (%(end_m)s IS NULL OR fr.month_id <= TO_DATE(%(end_m)s || '-01', 'YYYY-MM-DD'))
          AND fr.customer_id NOT IN (
            SELECT DISTINCT fr2.customer_id
            FROM core.fact_subscription_revenue fr2
            WHERE fr2.month_id < COALESCE(TO_DATE(%(start_m)s || '-01', 'YYYY-MM-DD'), DATE '1900-01-01')
          )
        GROUP BY fr.customer_id
    ),
    nl_periods AS (
      SELECT
        nl.customer_id,
        nl.first_month,
        -- compute period_start = GREATEST(first_month, start_m)
        GREATEST(
          nl.first_month,
          COALESCE(TO_DATE(%(start_m)s || '-01', 'YYYY-MM-DD'), nl.first_month)
        ) AS period_start,
        -- period_end is end_m if provided; otherwise use the last observed month for the customer
        COALESCE(
          TO_DATE(%(end_m)s || '-01', 'YYYY-MM-DD'),
          (SELECT MAX(fr2.month_id)
           FROM core.fact_subscription_revenue fr2
           WHERE fr2.customer_id = nl.customer_id)
        ) AS period_end
      FROM new_logos nl
    )
    SELECT
      nl.customer_id,
      -- months difference inclusive: (year diff)*12 + (month diff) + 1
      (
        (EXTRACT(YEAR FROM nl.period_end) - EXTRACT(YEAR FROM nl.period_start)) * 12
        + (EXTRACT(MONTH FROM nl.period_end) - EXTRACT(MONTH FROM nl.period_start))
        + 1
      )::INT AS months_in_period,
      SUM(fr.amount_lcy) AS revenue_in_period
    FROM nl_periods nl
    JOIN core.fact_subscription_revenue fr
      ON fr.customer_id = nl.customer_id
     AND fr.month_id BETWEEN nl.period_start AND nl.period_end
    GROUP BY nl.customer_id, nl.period_start, nl.period_end
    ORDER BY nl.customer_id;
    """

    return sql, params
