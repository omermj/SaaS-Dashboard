# Drill down on Subscription Revenues by Cohorts and Churn
# - MRR/ARR Trends: By product, region, billing cycle and customer cohorts
# - Churn Analysis: Customer and revenue churn rates, cohort retention analysis
# - Cohort Analysis: Revenue by signup cohort (monthly/annual) and retention heatmap
# - FX Normalization: Revenue in LCY vs USD

import streamlit as st
from db_engine import get_engine
from core.metrics.dim_data import get_all_products, get_all_countries, get_all_months


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


# ----- Top Row KPIs -----
# - Total MRR (for the period)
# - ARR
# - New Logos
# - Churned Logos
# - CAC Payback

st.subheader("Top Row KPIs")