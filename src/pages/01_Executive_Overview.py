import streamlit as st
from core.db import get_conn
from core.metrics import exec_overview_kpis, arr_bridge
from core.dim_data import get_all_products, get_all_countries, get_all_months
from ui.components import fmt_money, fmt_pct, fmt_months, fmt_multiple
import plotly.graph_objects as go


st.set_page_config(page_title="Executive Overview", layout="wide")
st.title("Executive Overview")


# Get product, country and month options from the database
@st.cache_data(ttl=600)
def load_dim_options():
    with get_conn() as conn:
        products = (
            get_all_products(conn).set_index("product_name").to_dict(orient="index")
        )
        countries = get_all_countries(conn)["country"].tolist()
        months = get_all_months(conn)["month"].tolist()
    months = sorted(months, reverse=True)
    return products, countries, months


products, countries, months = load_dim_options()
current_month = st.sidebar.selectbox("Current Month", options=months, index=0)

# ---- Sidebar Filters ----
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
c1.metric("ARR", fmt_money(global_kpis["arr"]), fmt_pct(global_kpis["arr_growth"]))
c2.metric("NRR", fmt_pct(global_kpis["nrr"]))
# c3.metric("GRR", fmt_pct(global_kpis["grr"]))
c4.metric("Net Monthly Burn", fmt_money(global_kpis["net_monthly_burn"]))

c5, c6, c7, c8 = st.columns(4)
c5.metric("Gross Margin", fmt_pct(global_kpis["gross_margin"]))
c6.metric("Op Margin", fmt_pct(global_kpis["op_margin"]))
c7.metric(
    "Burn Multiple",
    (
        "-"
        if global_kpis["burn_multiple"] == 0
        else fmt_multiple(global_kpis["burn_multiple"])
    ),
)
c8.metric("Runway Months", fmt_months(global_kpis["runway_months"]))
# c8.metric(
#     "Runway Months",
#     "∞" if global_kpis['runway_months'] >= 9999
#     else f"{global_kpis['runway_months']:.0f} mo"
# ),
# )


st.write("Ending Cash Balance", f"${global_kpis['ending_cash_balance']:,.0f}")

# ---- Section B: ARR Bridge ----
st.subheader("ARR Bridge")

# Prepare data for waterfall
steps = arr_bridge_data
waterfall = go.Figure(
    go.Waterfall(
        name="ARR Bridge",
        orientation="v",
        measure=steps["type"],
        x=steps["step"],
        y=steps["value"],
        connector={"line": {"color": "rgba(90,90,90,0.5)"}},
    )
)
waterfall.update_layout(
    title="ARR Waterfall Bridge",
    showlegend=False,
    margin=dict(l=20, r=20, t=40, b=20),
    height=400,
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

c1.metric("ARR", fmt_money(product_kpis["arr"]), fmt_pct(product_kpis["arr_growth"]))
c2.metric("NRR", fmt_pct(product_kpis["nrr"]))
c3.metric("GRR", fmt_pct(product_kpis["grr"]))
