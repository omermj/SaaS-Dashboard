import pandas as pd

df_fact_subscription_revenue = pd.read_csv(
    "data/fact_subscription_revenue.csv",
    parse_dates=["date_id"],
)
df_fact_cloud_cost = pd.read_csv(
    "data/fact_cloud_cost.csv",
    parse_dates=["date_id"],
)
df_marketing_spend = pd.read_csv(
    "data/fact_marketing_spend.csv",
    parse_dates=["date_id"],
)
df_other_expenses = pd.read_csv(
    "data/fact_other_expenses.csv",
    parse_dates=["date_id"],
)
df_payment_processing_cost = pd.read_csv(
    "data/fact_payment_processing_cost.csv",
    parse_dates=["date_id"],
)

START_DATE = "2022-01-01"
END_DATE = "2024-12-31"
INITIAL_CASH_BALANCE = 100000  # Example initial cash balance


def generate_cash_balance_table(
    df_fact_subscription_revenue,
    df_fact_cloud_cost,
    df_marketing_spend,
    df_other_expenses,
    df_payment_processing_cost,
):
    df_cash_balance = pd.DataFrame()

    # Create a date range for the cash balance table
    df_cash_balance["date_id"] = pd.date_range(start=START_DATE, end=END_DATE)

    # Set the initial cash balance on the start date
    df_cash_balance.loc[
        df_cash_balance["date_id"] == pd.Timestamp(START_DATE), "cash_balance"
    ] = INITIAL_CASH_BALANCE

    # Bring in the cash inflows from df_fact_subscription_revenue
    df_fact_subscription_revenue_grouped = (
        df_fact_subscription_revenue.groupby("date_id")["amount_lcy"]
        .sum()
        .reset_index()
    )
    df_cash_balance = df_cash_balance.merge(
        df_fact_subscription_revenue_grouped,
        on="date_id",
        how="left",
        suffixes=("", "_revenue"),
    )
    df_cash_balance.rename(columns={"amount_lcy": "revenue"}, inplace=True)
    df_cash_balance["revenue"] = df_cash_balance["revenue"].fillna(0)
    df_cash_balance["cash_in"] = df_cash_balance["revenue"].fillna(0)

    # Bring in the cash outflows from other expense tables
    for df, col_name in [
        (df_fact_cloud_cost, "cloud_cost"),
        (df_marketing_spend, "marketing_spend"),
        (df_other_expenses, "other_expenses"),
        (df_payment_processing_cost, "payment_processing_cost"),
    ]:
        df_grouped = df.groupby("date_id")["amount_lcy"].sum().reset_index()
        df_cash_balance = df_cash_balance.merge(
            df_grouped,
            on="date_id",
            how="left",
            suffixes=("", f"_{col_name}"),
        )
        df_cash_balance.rename(columns={"amount_lcy": col_name}, inplace=True)
        df_cash_balance[col_name] = df_cash_balance[col_name].fillna(0)

    df_cash_balance["cash_out"] = (
        df_cash_balance["cloud_cost"]
        + df_cash_balance["marketing_spend"]
        + df_cash_balance["other_expenses"]
        + df_cash_balance["payment_processing_cost"]
    )

    # Calculate the daily cash balance
    df_cash_balance["cash_balance"] = (
        INITIAL_CASH_BALANCE
        + df_cash_balance["cash_in"].cumsum()
        - df_cash_balance["cash_out"].cumsum()
    ).round(2)

    # Format date_id as YYYYMMDD string
    df_cash_balance["date_id"] = df_cash_balance["date_id"].dt.strftime("%Y%m%d")

    return df_cash_balance


if __name__ == "__main__":
    print("\n--- Generating Cash Balance Table ---")
    df_cash_balance = generate_cash_balance_table(
        df_fact_subscription_revenue,
        df_fact_cloud_cost,
        df_marketing_spend,
        df_other_expenses,
        df_payment_processing_cost,
    )

    print(f"Successfully generated {len(df_cash_balance)} cash balance records.")

    # Print the first 5 rows to verify the output
    print("\n--- Sample of Generated Cash Balance Table (First 5 rows) ---")
    print(df_cash_balance.head())

    # Print the last 5 rows to verify the output
    print("\n--- Sample of Generated Cash Balance Table (Last 5 rows) ---")
    print(df_cash_balance.tail())

    # --- Save the output to a CSV file ---
    output_filename = "data/fact_cash_balance.csv"
    try:
        df_cash_balance[["date_id", "cash_in", "cash_out", "cash_balance"]].to_csv(
            output_filename, index=False
        )
        print(f"\nCash balance table saved to {output_filename}")
    except Exception as e:
        print(f"Error saving cash balance table: {e}")
