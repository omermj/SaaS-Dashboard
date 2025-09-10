import pandas as pd
from datetime import datetime


def generate_date_dimension(start_date_str, end_date_str):
    """
    Generates a date dimension table for a given date range.

    Args:
        start_date_str (str): The start date in 'YYYY-MM-DD' format.
        end_date_str (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: A DataFrame containing the date dimension with the specified schema.
                      Returns None if the date formats are invalid.
    """
    try:
        # Validate and parse the input date strings
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        print("Error: Invalid date format. Please use 'YYYY-MM-DD'.")
        return None

    print(f"Generating date dimension from {start_date_str} to {end_date_str}...")

    # Create a date range using pandas
    date_range = pd.to_datetime(pd.date_range(start=start_date, end=end_date, freq="D"))

    # Create the initial DataFrame
    date_df = pd.DataFrame(date_range, columns=["date"])

    # --- Feature Engineering: Extract date components ---

    # date_id (string): YYYYMMDD format
    date_df["date_id"] = date_df["date"].dt.strftime("%Y%m%d")

    # year (number)
    date_df["year"] = date_df["date"].dt.year

    # quarter (number)
    date_df["quarter"] = date_df["date"].dt.quarter

    # month (number)
    date_df["month"] = date_df["date"].dt.month

    # day (number)
    date_df["day"] = date_df["date"].dt.day

    # Format the original date column to 'YYYY-MM-DD' string for consistency
    date_df["date"] = date_df["date"].dt.strftime("%Y-%m-%d")

    # Reorder columns to match the desired schema
    date_df = date_df[["date_id", "date", "year", "quarter", "month", "day"]]

    print(f"Successfully generated {len(date_df)} date records.")
    return date_df


# --- Main execution block ---
if __name__ == "__main__":
    # --- Configuration ---
    # Define the start and end dates for your date dimension.
    # For your Fintech SaaS company, this should start before the launch date.
    START_DATE = "2022-01-01"
    END_DATE = "2024-12-31"

    # Generate the date dimension table
    dim_date_df = generate_date_dimension(START_DATE, END_DATE)

    if dim_date_df is not None:
        # Print the first 5 rows to verify the output
        print("\n--- Sample of Generated Date Dimension (First 5 rows) ---")
        print(dim_date_df.head())

        # Print the last 5 rows to verify the output
        print("\n--- Sample of Generated Date Dimension (Last 5 rows) ---")
        print(dim_date_df.tail())

        # --- Save the output to a CSV file ---
        output_filename = "../data/dim_date.csv"
        try:
            dim_date_df.to_csv(output_filename, index=False)
            print(
                f"\nSuccessfully saved the full date dimension to '{output_filename}'"
            )
        except Exception as e:
            print(f"\nAn error occurred while saving the file: {e}")
