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
        parts.append("to_char(date_trunc('month', dd.date), 'YYYY-MM') >= %(start_m)s")
        params["start_m"] = start_month
    if end_month:
        parts.append("to_char(date_trunc('month', dd.date), 'YYYY-MM') <= %(end_m)s")
        params["end_m"] = end_month
    where = (" WHERE " + " AND ".join(parts)) if parts else ""
    return where, params
