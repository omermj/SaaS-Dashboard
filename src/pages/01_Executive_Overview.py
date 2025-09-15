import streamlit as st
from core.db import get_conn
from core.metrics import exec_overview_kpis
from core.dim_data import get_all_products, get_all_countries, get_all_months
import pandas as pd


st.set_page_config(page_title="Executive Overview", layout="wide")
st.title("Executive Overview")


# Get product, country and month options from the database
products = (
    get_all_products(get_conn()).set_index("product_name").to_dict(orient="index")
)
countries = get_all_countries(get_conn())["country"].tolist()
months = get_all_months(get_conn())["month"].tolist()

# ---- Sidebar Filters ----
current_month = st.sidebar.selectbox("Current Month", options=months, index=0)

st.sidebar.header("Filters")

product_name = st.sidebar.selectbox(
    "Product Name",
    options=["All"] + [p for p in products.keys()],
    index=0,
)
country = st.sidebar.selectbox(
    "Country",
    options=["All"] + countries,
    index=0,
)
time_range = st.sidebar.radio("Time Range", options=["Last 12M", "YTD", "QTD"], index=1)

# Get product_id from product_name
product_id = (
    products.get(product_name, {}).get("product_id") if product_name != "All" else None
)

# ---- Load Data ----
with get_conn() as conn:
    kpis = exec_overview_kpis(
        conn,
        product_id=product_id,
        country=country,
        time_range=time_range,
        end_month=current_month,
    )


# ---- Section A: North Star KPIs ----
st.subheader("North Star KPIs")
c1, c2, c3 = st.columns(3)
c1.metric("ARR", f"${kpis['arr']:,.0f}", f"{kpis['arr_growth']:.1%}")
