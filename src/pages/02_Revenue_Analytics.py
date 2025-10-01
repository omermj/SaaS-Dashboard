# Drill down on Subscription Revenues by Cohorts and Churn
# - MRR/ARR Trends: By product, region, billing cycle and customer cohorts
# - Churn Analysis: Customer and revenue churn rates, cohort retention analysis
# - Cohort Analysis: Revenue by signup cohort (monthly/annual) and retention heatmap
# - FX Normalization: Revenue in LCY vs USD

import streamlit as st
from db_engine import get_engine
from core.metrics.dim_data import get_all_products, get_all_countries, get_all_months
from core.metrics.metrics_revenue_analytics import (
    top_row_kpis,
)
from ui.components import (
    fmt_money,
    fmt_pct,
    fmt_months,
    fmt_multiple,
    fmt_margin,
    fmt_number,
)

engine = get_engine()

# Set page config
st.set_page_config(page_title="Revenue Analytics", layout="wide")
st.title("Revenue Analytics")


# Get product, country and month options from the database
@st.cache_data(ttl=600)
def load_dim_options():
    with engine.begin() as conn:
        products = (
            get_all_products(conn).set_index("product_name").to_dict(orient="index")
        )
        countries = get_all_countries(conn)["country"].tolist()
        months = get_all_months(conn)["month"].tolist()
    months = sorted(months, reverse=True)
    return products, countries, months


products, countries, months = load_dim_options()

# ---- Sidebar Filters ----
current_month = st.sidebar.selectbox("Current Month", options=months, index=0)
st.sidebar.header("Filters")
time_range = st.sidebar.radio("Time Range", options=["Last 12M", "YTD", "QTD"], index=0)
product_filter = st.sidebar.selectbox(
    "Product", options=["All"] + list(products.keys()), index=0
)
country_filter = st.sidebar.selectbox("Country", options=["All"] + countries, index=0)
billing_cycle_filter = st.sidebar.selectbox(
    "Billing Cycle", options=["All", "Monthly", "Annual"], index=0
)
# ---- Load Data ----
with engine.begin() as conn:
    top_row_metrics = top_row_kpis(
        conn,
        product_id=(
            products[product_filter]["product_id"] if product_filter != "All" else None
        ),
        country=country_filter if country_filter != "All" else None,
        billing_cycle=(
            billing_cycle_filter.lower() if billing_cycle_filter != "All" else None
        ),
        time_range=time_range,
        end_month=current_month,
    )

# ----- Top Row KPIs -----
# - Total MRR (for the period)
# - ARR (Latest Month MRR x 12)
# - New Logos
# - Churned Logos
# - CAC Payback

st.subheader("Top Row KPIs")
c1, c2, c3, c4, c5 = st.columns(5)

c1.metric(
    label="MRR",
    value=fmt_money(top_row_metrics["mrr"]),
    help="Total MRR for the selected time range",
)
c2.metric(
    label="ARR",
    value=fmt_money(top_row_metrics["arr"]),
    help="ARR (Latest Month MRR x 12)",
)
c3.metric(
    label="New Logos",
    value=fmt_number(top_row_metrics["new_logos"]),
    help="Number of new logos acquired in the selected time range",
)
c4.metric(
    label="Churned Logos",
    value=fmt_number(top_row_metrics["churned_logos"]),
    help="Number of logos churned in the selected time range",
)
c5.metric(
    label="CAC Payback",
    value=fmt_months(top_row_metrics["cac_payback"]),
    help="CAC Payback Period (in months)",
)
