from __future__ import annotations

from pathlib import Path
import pandas as pd


def map_price_to_subscription_data(
    fact_subscription_path: str | Path = "data/fact_subscription_revenue.csv",
    product_dimension_path: str | Path = "data/dim_product.csv",
) -> pd.DataFrame:
    """Map product pricing onto the subscription revenue fact table and return the updated DataFrame."""
    fact_subscription_path = Path(fact_subscription_path)
    product_dimension_path = Path(product_dimension_path)

    fact_df = pd.read_csv(fact_subscription_path, parse_dates=["date_id"])
    product_df = pd.read_csv(product_dimension_path)

    merged_df = fact_df.merge(
        product_df[["product_id", "price_monthly", "price_annual"]],
        on="product_id",
        how="left",
    )

    monthly_mask = merged_df["billing_cycle"] == "monthly"
    annual_mask = merged_df["billing_cycle"] == "annual"

    merged_df.loc[monthly_mask, "amount_lcy"] = merged_df.loc[
        monthly_mask, "price_monthly"
    ]
    merged_df.loc[annual_mask, "amount_lcy"] = merged_df.loc[
        annual_mask, "price_annual"
    ]

    merged_df = merged_df.drop(columns=["price_monthly", "price_annual"])

    # Format date_id as YYYYMMDD string
    merged_df["date_id"] = merged_df["date_id"].dt.strftime("%Y%m%d")

    return merged_df


def main() -> None:
    updated_df = map_price_to_subscription_data()
    updated_df.to_csv("data/fact_subscription_revenue.csv", index=False)
    print("Updated fact_subscription_revenue.csv with mapped prices.")
    print(updated_df.head())


if __name__ == "__main__":
    main()
