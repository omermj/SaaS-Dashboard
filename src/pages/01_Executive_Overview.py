import streamlit as st
from core.db import get_conn
from core.metrics import exec_overview_kpis, arr_bridge
from core.dim_data import get_all_products, get_all_countries, get_all_months
import plotly.graph_objects as go


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
time_range = st.sidebar.radio("Time Range", options=["Last 12M", "YTD", "QTD"], index=0)

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
    arr_bridge_data = arr_bridge(
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
c2.metric("NRR", f"{kpis['nrr']:.1%}")
c3.metric("GRR", f"{kpis['grr']:.1%}")

c4, c5, c6 = st.columns(3)
c4.metric("Gross Margin", f"{kpis['gross_margin']:.1%}")
c5.metric("Op Margin", f"{kpis['op_margin']:.1%}")
c6.metric(
    "Burn Multiple",
    f"{kpis['burn_multiple']:.2f}",
    f"{kpis['runway_months']:.0f} months",
)


# ---- Section B: ARR Bridge ----
st.subheader("ARR Bridge")

# Prepare data for waterfall
waterfall = go.Figure(
    go.Waterfall(
        name="ARR Bridge",
        orientation="v",
        measure=[
            "relative" if (t != "total") else "total"
            for t in arr_bridge_data.get("type", ["relative"] * len(arr_bridge_data))
        ],
        x=arr_bridge_data["step"],
        y=arr_bridge_data["value"],
        connector={"line": {"color": "rgb(63, 63, 63)"}},
    )
)

# Set y-axis to not start from 0
y_min = 205000  # arr_bridge_data["value"].min()
y_max = arr_bridge_data["value"].max()
buffer = (y_max - y_min) * 0.1  # 10% buffer
waterfall.update_layout(
    title="ARR Waterfall Bridge",
    showlegend=False,
    margin=dict(l=20, r=20, t=40, b=20),
    height=400,
    yaxis=dict(range=[y_min - buffer, y_max + buffer]),
)

st.plotly_chart(waterfall, use_container_width=True)
