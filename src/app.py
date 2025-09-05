import streamlit as st
import pandas as pd
import plotly.express as px


# Page title
st.set_page_config(page_title="Startup FP&A Dashboard")

# App title
st.title("Startup FP&A Dashboard")


# Load data
# @st.cache_data
def load_data():
    df = pd.read_csv("data/mock_dashboard_data.csv")
    df["month"] = pd.to_datetime(df["month"], format="%b-%Y")
    return df


df = load_data()

# Sidebar controls
st.sidebar.header("Scenario Controls")

# Adjust Churn Rate
churn_adjustment = st.sidebar.slider(
    "Adjust Churn Rate (%)",
    min_value=-2.0,
    max_value=2.0,
    value=0.0,
    step=0.1,
    format="%.1f",
)

# Adjust Headcount Expenses
headcount_adjustment = st.sidebar.slider(
    "Adjust Headcount Expenses (%)",
    min_value=-2.0,
    max_value=2.0,
    value=0.0,
    step=0.1,
)

# Show the dataframe
st.subheader("Raw Data Preview")
st.dataframe(df.head())

# Apply churn rate adjustment
df["adjusted_churn_rate"] = df["churn_rate"] + churn_adjustment / 100
df["adjusted_churn_rate"] = df["adjusted_churn_rate"].clip(
    lower=0.0
)  # no negative churn

# Adjust headcount expenses
df["adjusted_expenses_headcount"] = df["expenses_headcount"] * (
    1 + headcount_adjustment / 100
)

# Update total expenses and burn rate with adjustments
df["adjusted_total_expenses"] = df[
    [
        "adjusted_expenses_headcount",
        "expenses_marketing",
        "expenses_tools",
        "expenses_gna",
    ]
].sum(axis=1)
df["adjusted_burn_rate"] = df["adjusted_total_expenses"] - df["mrr"] * 0.9

# --- KPI Section ---
st.subheader("Key Metrics")

# Latest MRR
latest_mrr = df["mrr"].iloc[-1]

# Average churn rate
avg_churn_rate = df["churn_rate"].mean() * 100

# Latest cash balance
latest_cash = df["cash_balance"].iloc[-1]

# --- Runway Calculation ---
# Use adjusted burn rate average over the last 3 months
avg_burn_rate = -df["adjusted_burn_rate"].tail(3).mean()

# Project against divide by 0
if avg_burn_rate > 0:
    runaway_months = latest_cash / avg_burn_rate
else:
    runaway_months = float("inf")

# Display Runway
st.subheader("Estimated Runway")
if runaway_months == float("inf"):
    st.write("Runway is infinite (no burn rate).")
else:
    st.write(f"Estimated Runway: {runaway_months:.1f} months")

# Display in 3 columns
col1, col2, col3 = st.columns(3)
col1.metric("Latest MRR", f"${latest_mrr:,.0f}")
col2.metric("Avg. Churn Rate", f"{avg_churn_rate:.2f}%")
col3.metric("Cash Balance", f"${latest_cash:,.0f}")

# --- MRR Line Chart ---
st.subheader("Monthly Recurring Revenue (MRR) Over Time")

fig_mrr = px.line(
    df,
    x="month",
    y="mrr",
    title="MRR Trend",
    markers=True,
)

fig_mrr.update_layout(xaxis_title="Month", yaxis_title="MRR ($)")

st.plotly_chart(fig_mrr, use_container_width=True)

# --- Calculate Burn Rate ---
# Total Expenses
df["total_expenses"] = df[
    ["expenses_headcount", "expenses_marketing", "expenses_tools", "expenses_gna"]
].sum(axis=1)

# Assume 90% MRR is collected as cash inflow
df["burn_rate"] = df["total_expenses"] - df["mrr"] * 0.9

# --- Burn Rate Line Chart ---
st.subheader("Burn Rate Over Time")
fig_burn_rate = px.line(
    df,
    x="month",
    y="adjusted_burn_rate",
    title="Burn Rate Trend with Scenario Adjustments",
    markers=True,
)
fig_burn_rate.update_layout(xaxis_title="Month", yaxis_title="Burn Rate ($)")
st.plotly_chart(fig_burn_rate, use_container_width=True)

# --- Cash Balance Line Chart ---
st.subheader("Cash Balance Over Time")

fig_cash_balance = px.line(
    df,
    x="month",
    y="cash_balance",
    title="Cash Balance Trend",
    markers=True,
)
fig_cash_balance.update_layout(xaxis_title="Month", yaxis_title="Cash Balance ($)")
st.plotly_chart(fig_cash_balance, use_container_width=True)
