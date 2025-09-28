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


time_range = st.sidebar.radio("Time Range", options=["Last 12M", "YTD", "QTD"], index=0)

# ---- Load Data ----
with get_conn() as conn:
    global_kpis = exec_overview_kpis(
        conn,
        time_range=time_range,
        end_month=current_month,
    )
    arr_bridge_data = arr_bridge(
        conn,
        time_range=time_range,
        end_month=current_month,
    )


# ---- Section A: North Star KPIs ----
st.subheader("North Star KPIs")
c1, c2, c3, c4 = st.columns(4)
c1.metric("ARR", f"${global_kpis['arr']:,.0f}", f"{global_kpis['arr_growth']:.1%}")
c2.metric("NRR", f"{global_kpis['nrr']:.1%}")
c3.metric("GRR", f"{global_kpis['grr']:.1%}")
c4.metric("Net Monthly Burn", f"${global_kpis['net_monthly_burn']:,.0f}")

c5, c6, c7, c8 = st.columns(4)
c5.metric("Gross Margin", f"{global_kpis['gross_margin']:.1%}")
c6.metric("Op Margin", f"{global_kpis['op_margin']:.1%}")
c7.metric("Ending Cash Balance", f"${global_kpis['ending_cash_balance']:,.0f}")
c8.metric(
    "Burn Multiple",
    f"{global_kpis['burn_multiple']:.2f}",
    f"{global_kpis['runway_months']:.0f} months",
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
y_min = 3000000  # arr_bridge_data["value"].min()
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

st.divider()

# ---- Section C: Product KPIs ----
st.subheader("Product KPIs")

c1, c2 = st.columns(2)

# Filters for product and country
product_name = c1.selectbox(
    "Product Name",
    options=["All"] + [p for p in products.keys()],
    index=0,
)
country = c2.selectbox(
    "Country",
    options=["All"] + countries,
    index=0,
)

# Get product_id from product_name
product_id = (
    products.get(product_name, {}).get("product_id") if product_name != "All" else None
)

# Load product-specific KPIs if a specific product is selected
with get_conn() as conn:
    product_kpis = exec_overview_kpis(
        conn,
        product_id=product_id,
        country=country,
        time_range=time_range,
        end_month=current_month,
    )

c1, c2, c3 = st.columns(3)

c1.metric("ARR", f"${product_kpis['arr']:,.0f}", f"{product_kpis['arr_growth']:.1%}")
c2.metric("NRR", f"{product_kpis['nrr']:.1%}")
c3.metric("GRR", f"{product_kpis['grr']:.1%}")
