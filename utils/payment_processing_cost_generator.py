import pandas as pd
import random
import uuid

# --- Configuration: Define Payment Processors and Their Rates ---
PROCESSORS = [
    {
        "name": "Stripe",
        "rate": 0.029,  # 2.9%
        "fixed_fee": 0.30,  # $0.30
        "weight": 0.60,  # 60% of transactions
    },
    {
        "name": "Adyen",
        "rate": 0.027,  # 2.7% (example rate, can vary)
        "fixed_fee": 0.35,  # $0.35 (example rate)
        "weight": 0.25,  # 25% of transactions
    },
    {
        "name": "Braintree",
        "rate": 0.0259,  # 2.59%
        "fixed_fee": 0.49,  # $0.49
        "weight": 0.15,  # 15% of transactions
    },
]


def generate_processing_costs(subscription_df):
    """
    Generates payment processing cost data based on subscription revenue data.

    Args:
        subscription_df (pd.DataFrame): DataFrame with subscription revenue data.
                                        Must contain 'date_id', 'customer_id',
                                        'amount', and 'currency_code' columns.

    Returns:
        pd.DataFrame: A new DataFrame with the schema for fact_payment_processing_cost.
    """
    print("Starting generation of payment processing costs...")

    processing_costs_data = []

    # Extract processor names and weights for random selection
    processor_names = [p["name"] for p in PROCESSORS]
    processor_weights = [p["weight"] for p in PROCESSORS]

    # Create a mapping from name to details for quick lookup
    processor_map = {p["name"]: p for p in PROCESSORS}

    # Iterate over each subscription record to generate a corresponding cost record
    for index, row in subscription_df.iterrows():
        # 1. Randomly choose a payment processor based on the defined weights
        chosen_processor_name = random.choices(
            processor_names, weights=processor_weights, k=1
        )[0]
        processor_details = processor_map[chosen_processor_name]

        # 2. Get the revenue amount for the transaction
        subscription_amount = row["amount_lcy"]

        # 3. Calculate the processing fee
        # Formula: (Transaction Amount * Percentage Rate) + Fixed Fee
        processing_fee = (
            subscription_amount * processor_details["rate"]
        ) + processor_details["fixed_fee"]

        # 4. Create the new record for the fact_payment_processing_cost table
        cost_record = {
            "source_system": "csv",
            "sub_source_record_id": row.get("source_record_id"),
            "source_record_id": str(uuid.uuid4()),  # Unique ID for this cost record
            "date_id": row["date_id"],
            "processor_name": chosen_processor_name,
            "amount_lcy": round(
                processing_fee, 2
            ),  # Round to 2 decimal places for currency
            "currency_code": row["currency_code"],
            "ingest_batch_id": "batch_001",  # Example batch ID
        }

        processing_costs_data.append(cost_record)

    print(
        f"Successfully generated {len(processing_costs_data)} processing cost records."
    )
    return pd.DataFrame(processing_costs_data)


# --- Main execution block ---
if __name__ == "__main__":
    print("\n--- Running on Your Data (from CSV) ---")
    try:
        # Load your subscription revenue data from a CSV file
        # Make sure the file 'fact_subscription_revenue.csv' is in the same directory as the script.
        revenue_df_actual = pd.read_csv("../data/fact_subscription_revenue.csv")

        # Generate the payment processing costs
        processing_costs_df_actual = generate_processing_costs(revenue_df_actual)

        # Save the generated data to a new CSV file
        output_filename = "../data/fact_payment_processing_cost.csv"
        processing_costs_df_actual.to_csv(output_filename, index=False)

        print(f"\nSuccessfully saved the generated data to '{output_filename}'")

    except FileNotFoundError:
        print("\nERROR: 'fact_subscription_revenue.csv' not found.")
        print(
            "Please place your revenue data file in the same directory as this script."
        )
    except Exception as e:
        print(f"\nAn error occurred: {e}")
