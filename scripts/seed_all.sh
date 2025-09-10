#!/usr/bin/env bash
set -euo pipefail

CONN="postgresql://saas_user:saas_password@localhost:5433/saas_dashboard"

psql "$CONN" -f db/init.sql

psql "$CONN" -c "\copy staging.dim_currency FROM 'data/dim_currency.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.dim_customer FROM 'data/dim_customer.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.dim_date FROM 'data/dim_date.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.dim_department FROM 'data/dim_department.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.dim_employee FROM 'data/dim_employee.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.dim_other_expense_type FROM 'data/dim_other_expense_type.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.dim_product FROM 'data/dim_product.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.fact_cloud_cost FROM 'data/fact_cloud_cost.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.fact_fx_rate FROM 'data/fact_fx_rate.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.fact_marketing_spend FROM 'data/fact_marketing_spend.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.fact_other_expenses FROM 'data/fact_other_expenses.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.fact_subscription_revenue \
(source_system, source_record_id, date_id, customer_id, product_id, billing_cycle, amount_lcy, currency_code, country, ingest_batch_id) \
FROM 'data/fact_subscription_revenue.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"
psql "$CONN" -c "\copy staging.fact_payment_processing_cost \
(source_system, sub_source_record_id, source_record_id, date_id, processor_name, amount_lcy, currency_code, ingest_batch_id) \
FROM 'data/fact_payment_processing_cost.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');"

psql "$CONN" -f db/transform_upsert.sql

echo "Seeded successfully!"