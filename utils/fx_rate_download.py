import pandas as pd
import requests
import time
from datetime import datetime, timedelta


def get_daily_fx_rates(start_date_str, end_date_str, base_currency="USD"):
    """
    Fetches daily foreign exchange rates using the Frankfurter.app API.

    Args:
        start_date_str (str): The start date in 'YYYY-MM-DD' format.
        end_date_str (str): The end date in 'YYYY-MM-DD' format.
        base_currency (str): The base currency for the rates (default is 'USD').

    Returns:
        pd.DataFrame: A DataFrame with daily FX rates, or None if an error occurs.
    """
    # --- Configuration ---
    # Currencies you want to get rates for, against the base currency.
    target_currencies = ["CAD", "EUR", "GBP", "JPY"]

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        print("Error: Invalid date format. Please use 'YYYY-MM-DD'.")
        return None

    print(
        f"Fetching daily FX rates from {start_date_str} to {end_date_str} using Frankfurter.app API..."
    )

    all_rates_data = []
    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        # Use the Frankfurter.app API endpoint for historical data
        api_url = f"https://api.frankfurter.app/{date_str}?from={base_currency}&to={','.join(target_currencies)}"

        try:
            response = requests.get(api_url)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
            data = response.json()

            rates = data.get("rates", {})
            date_id = current_date.strftime("%Y%m%d")

            print("Downloading data for date:", date_str)

            for currency in target_currencies:
                if currency in rates:
                    rate_record = {
                        "date_id": date_id,
                        "currency_code": currency,
                        "rate_to_usd": rates[currency],
                    }
                    all_rates_data.append(rate_record)
                else:
                    print(
                        f"Warning: Rate for {currency} not found for date {date_str}."
                    )

        except requests.exceptions.RequestException as e:
            print(f"An error occurred during API request for {date_str}: {e}")
            # Optional: decide if you want to stop or continue on error
            # return None

        # Be a good citizen by adding a small delay to avoid overwhelming the free API
        time.sleep(0.1)

        # Move to the next day
        current_date += timedelta(days=1)

    if not all_rates_data:
        print("No data was fetched. Please check your date range.")
        return None

    print(f"Successfully fetched {len(all_rates_data)} FX rate records.")
    return pd.DataFrame(all_rates_data)


# --- Main execution block ---
if __name__ == "__main__":
    # --- Configuration ---
    # Define the date range for which you want FX rates.
    START_DATE = "2022-01-01"
    END_DATE = "2024-12-31"

    # Generate the FX rates table (no API key needed)
    fact_fx_df = get_daily_fx_rates(START_DATE, END_DATE)

    if fact_fx_df is not None:
        # Print the first 5 rows to verify the output
        print("\n--- Sample of Generated FX Rates (First 5 rows) ---")
        print(fact_fx_df.head())

        # Print the last 5 rows to verify the output
        print("\n--- Sample of Generated FX Rates (Last 5 rows) ---")
        print(fact_fx_df.tail())

        # --- Save the output to a CSV file ---
        output_filename = "../data/fact_fx_rate.csv"
        try:
            fact_fx_df.to_csv(output_filename, index=False)
            print(f"\nSuccessfully saved the full FX rate table to '{output_filename}'")
        except Exception as e:
            print(f"\nAn error occurred while saving the file: {e}")
