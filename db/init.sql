-- =========================================================
-- Schemas
-- =========================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;

-- Drop all tables in both schemas to reset the state if condition is true
DO $$
DECLARE
    v_schema_name text;
    v_table_name text;
    condition boolean := true; -- Set your condition here
BEGIN
    IF condition THEN
        FOR v_schema_name IN SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('staging', 'core') LOOP
            FOR v_table_name IN SELECT table_name FROM information_schema.tables WHERE table_schema = v_schema_name LOOP
                EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', v_schema_name, v_table_name);
            END LOOP;
        END LOOP;
    END IF;
END $$;


-- =========================================================
-- CORE TABLES
-- =========================================================

-- =========================================================
-- Dimensions
-- =========================================================
-- Table: dim_product
CREATE TABLE IF NOT EXISTS core.dim_product (
  product_id TEXT PRIMARY KEY,
  product_name TEXT NOT NULL,
  product_type TEXT NOT NULL,
  currency TEXT NOT NULL,
  price_monthly NUMERIC(18,2) NOT NULL,
  price_annual NUMERIC(18,2) NOT NULL
);

-- Table: dim_customer
CREATE TABLE IF NOT EXISTS core.dim_customer (
  customer_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  country TEXT NOT NULL,
  region TEXT NOT NULL,
  signup_date DATE NOT NULL,
  is_active BOOLEAN NOT NULL
);

-- Table: dim_date
CREATE TABLE IF NOT EXISTS core.dim_date (
  date_id DATE PRIMARY KEY,
  date DATE NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  month INT NOT NULL,
  day INT NOT NULL
);

-- Table: dim_currency
CREATE TABLE IF NOT EXISTS core.dim_currency (
  currency_code TEXT PRIMARY KEY,
  currency_name TEXT NOT NULL
);

-- Table: dim_department
CREATE TABLE IF NOT EXISTS core.dim_department (
  department_id TEXT PRIMARY KEY,
  department_name TEXT NOT NULL
);

--  Table: dim_employee
CREATE TABLE IF NOT EXISTS core.dim_employee (
  employee_id TEXT PRIMARY KEY,
  employee_name TEXT NOT NULL,
  hire_date DATE NOT NULL,
  termination_date DATE,
  department_id TEXT NOT NULL REFERENCES core.dim_department(department_id),
  country TEXT NOT NULL,
  title TEXT NOT NULL,
  salary NUMERIC(18,2) NOT NULL
);

-- Table: dim_other_expense_type
CREATE TABLE IF NOT EXISTS core.dim_other_expense_type (
  other_expense_type_id TEXT PRIMARY KEY,
  expense_type_name TEXT NOT NULL,
  expense_description TEXT NOT NULL
);

-- =========================================================
-- Facts
-- =========================================================
-- Table: fact_fx_rate
CREATE TABLE IF NOT EXISTS core.fact_fx_rate (
  date_id DATE NOT NULL REFERENCES core.dim_date(date_id),
  currency_code TEXT NOT NULL REFERENCES core.dim_currency(currency_code),
  rate_to_usd NUMERIC(18,6) NOT NULL,
  CONSTRAINT pk_fx PRIMARY KEY (date_id, currency_code)
);

-- Table: fact_subscription_revenue
CREATE TABLE IF NOT EXISTS core.fact_subscription_revenue (
  fact_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  date_id DATE NOT NULL REFERENCES core.dim_date(date_id),
  customer_id TEXT NOT NULL REFERENCES core.dim_customer(customer_id),
  product_id TEXT NOT NULL REFERENCES core.dim_product(product_id),
  billing_cycle TEXT NOT NULL,
  amount_lcy NUMERIC(18,2) NOT NULL,
  currency_code TEXT NOT NULL REFERENCES core.dim_currency(currency_code),
  country TEXT NOT NULL,
  
  -- lineage 
  source_system TEXT,
  source_record_id TEXT,
  ingest_batch_id TEXT,
  ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT fk_subrev_fx
    FOREIGN KEY (date_id, currency_code)
    REFERENCES core.fact_fx_rate(date_id, currency_code)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS ix_subrev_slide
  ON core.fact_subscription_revenue (date_id, customer_id, product_id, billing_cycle);

-- Derived Table: fact_subscription from fact_subscription_revenue
CREATE TABLE IF NOT EXISTS core.fact_subscription (
  subscription_id BIGSERIAL PRIMARY KEY, --Check this
  customer_id TEXT NULL,
  product_id TEXT NULL,
  billing_cycle TEXT NULL,
  price_per_period NUMERIC(18,2) NOT NULL,
  mrr_value NUMERIC(18,2) NOT NULL,
  currency_code TEXT NOT NULL,
  start_date TEXT NOT NULL,
  end_date TEXT,
  status TEXT NOT NULL
);

-- Create index
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_subscription_mvp
  ON core.fact_subscription (
    customer_id, 
    product_id, 
    billing_cycle,
    price_per_period,
    currency_code,
    start_date
);

-- Derived Table: fact_subscription_snapshot_monthly from fact_subscription
CREATE TABLE IF NOT EXISTS core.fact_subscription_snapshot_monthly (
  snapshot_month DATE NOT NULL,
  subscription_id BIGINT NOT NULL REFERENCES core.fact_subscription(subscription_id),
  customer_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  mrr_value NUMERIC(18,2) NOT NULL,
  PRIMARY KEY (snapshot_month, subscription_id)
);

-- Table: fact_cloud_cost
CREATE TABLE IF NOT EXISTS core.fact_cloud_cost (
  cloud_cost_id TEXT PRIMARY KEY,
  date_id DATE NOT NULL REFERENCES core.dim_date(date_id),
  provider_name TEXT NOT NULL,
  amount_lcy NUMERIC(18,2) NOT NULL,
  currency_code TEXT NOT NULL REFERENCES core.dim_currency(currency_code),
  CONSTRAINT fk_cloud_fx
    FOREIGN KEY (date_id, currency_code)
    REFERENCES core.fact_fx_rate(date_id, currency_code)
    DEFERRABLE INITIALLY DEFERRED
);

-- Table: fact_payment_processing_cost
CREATE TABLE IF NOT EXISTS core.fact_payment_processing_cost (
  payment_proc_cost_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  date_id DATE NOT NULL REFERENCES core.dim_date(date_id),
  processor_name TEXT NOT NULL,
  transaction_id BIGINT NOT NULL REFERENCES core.fact_subscription_revenue(fact_id),
  amount_lcy NUMERIC(18,2) NOT NULL,
  currency_code TEXT NOT NULL REFERENCES core.dim_currency(currency_code),

  -- lineage
  source_system TEXT,
  source_record_id TEXT,
  sub_source_record_id TEXT,
  ingest_batch_id TEXT,
  ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT fk_payproc_fx
    FOREIGN KEY (date_id, currency_code)
    REFERENCES core.fact_fx_rate(date_id, currency_code)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS ix_payproc_date_ccy
  ON core.fact_payment_processing_cost (date_id, currency_code);

-- Table: fact_other_expenses
CREATE TABLE IF NOT EXISTS core.fact_other_expenses (
  other_expense_id TEXT PRIMARY KEY,
  date_id DATE NOT NULL REFERENCES core.dim_date(date_id),
  other_expense_type_id TEXT NOT NULL REFERENCES core.dim_other_expense_type(other_expense_type_id),
  vendor_name TEXT NOT NULL,
  invoice_number TEXT NOT NULL,
  amount_lcy NUMERIC(18,2) NOT NULL,
  currency_code TEXT NOT NULL REFERENCES core.dim_currency(currency_code),
  CONSTRAINT fk_other_fx
    FOREIGN KEY (date_id, currency_code)
    REFERENCES core.fact_fx_rate(date_id, currency_code)
    DEFERRABLE INITIALLY DEFERRED
);

-- Table: fact_marketing_spend
CREATE TABLE IF NOT EXISTS core.fact_marketing_spend (
  marketing_spend_id TEXT PRIMARY KEY,
  date_id DATE NOT NULL REFERENCES core.dim_date(date_id),
  channel TEXT NOT NULL,
  campaign_id TEXT NOT NULL,
  amount_lcy NUMERIC(18,2) NOT NULL,
  currency_code TEXT NOT NULL REFERENCES core.dim_currency(currency_code),
  CONSTRAINT fk_marketing_fx
    FOREIGN KEY (date_id, currency_code)
    REFERENCES core.fact_fx_rate(date_id, currency_code)
    DEFERRABLE INITIALLY DEFERRED
);

-- Table: fact_cash_balance
CREATE TABLE IF NOT EXISTS core.fact_cash_balance (
  cash_balance_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  date_id DATE NOT NULL REFERENCES core.dim_date(date_id),
  cash_in NUMERIC(18,2),
  cash_out NUMERIC(18,2),
  cash_balance NUMERIC(18,2) NOT NULL
);
-- index
CREATE UNIQUE INDEX IF NOT EXISTS ix_cash_balance_date
  ON core.fact_cash_balance (date_id);


-- =========================================================
-- STAGING TABLES
-- =========================================================

-- =========================================================
-- Dimensions
-- =========================================================
-- Table: dim_product
CREATE TABLE IF NOT EXISTS staging.dim_product (
  product_id TEXT,
  product_name TEXT,
  product_type TEXT,
  currency TEXT,
  price_monthly TEXT,
  price_annual TEXT
);

-- Table: dim_customer
CREATE TABLE IF NOT EXISTS staging.dim_customer (
  customer_id TEXT,
  name TEXT,
  email TEXT,
  country TEXT,
  region TEXT,
  signup_date TEXT,
  is_active TEXT
);

-- Table: dim_date
CREATE TABLE IF NOT EXISTS staging.dim_date (
  date_id TEXT,
  date TEXT,
  year TEXT,
  quarter TEXT,
  month TEXT,
  day TEXT
);

-- Table: dim_currency
CREATE TABLE IF NOT EXISTS staging.dim_currency (
  currency_code TEXT,
  currency_name TEXT
);

-- Table: dim_department
CREATE TABLE IF NOT EXISTS staging.dim_department (
  department_id TEXT,
  department_name TEXT
);

--  Table: dim_employee
CREATE TABLE IF NOT EXISTS staging.dim_employee (
  employee_id TEXT,
  employee_name TEXT,
  hire_date TEXT,
  termination_date TEXT,
  department_id TEXT,
  country TEXT,
  title TEXT,
  salary TEXT
);

-- Table: dim_other_expense_type
CREATE TABLE IF NOT EXISTS staging.dim_other_expense_type (
  other_expense_type_id TEXT,
  expense_type_name TEXT,
  expense_description TEXT
);

-- =========================================================
-- Facts
-- =========================================================
-- Table: fact_fx_rate
CREATE TABLE IF NOT EXISTS staging.fact_fx_rate (
  date_id TEXT,
  currency_code TEXT,
  rate_to_usd TEXT
);

-- Table: fact_subscription_revenue
CREATE TABLE IF NOT EXISTS staging.fact_subscription_revenue (
  source_system TEXT,
  source_record_id TEXT,
  date_id TEXT,
  customer_id TEXT,
  product_id TEXT,
  billing_cycle TEXT,
  amount_lcy TEXT,
  currency_code TEXT,
  country TEXT,

  -- lineage
  ingest_batch_id TEXT,
  ingest_ts TIMESTAMPTZ DEFAULT now()
);

-- Table: fact_cloud_cost
CREATE TABLE IF NOT EXISTS staging.fact_cloud_cost (
  cloud_cost_id TEXT,
  date_id TEXT,
  provider_name TEXT,
  amount_lcy TEXT,
  currency_code TEXT
);

-- Table: fact_payment_processing_cost
CREATE TABLE IF NOT EXISTS staging.fact_payment_processing_cost (
  date_id TEXT,
  processor_name TEXT,
  transaction_id TEXT,
  amount_lcy TEXT,
  currency_code TEXT,

  -- lineage
  source_system TEXT,
  sub_source_record_id TEXT,
  source_record_id TEXT,
  ingest_batch_id TEXT,
  ingest_ts TIMESTAMPTZ DEFAULT now()
);

-- Table: fact_other_expenses
CREATE TABLE IF NOT EXISTS staging.fact_other_expenses (
  other_expense_id TEXT,
  date_id TEXT,
  other_expense_type_id TEXT,
  vendor_name TEXT,
  invoice_number TEXT,
  amount_lcy TEXT,
  currency_code TEXT
);

-- Table: fact_marketing_spend
CREATE TABLE IF NOT EXISTS staging.fact_marketing_spend (
  marketing_spend_id TEXT,
  date_id TEXT,
  channel TEXT,
  campaign_id TEXT,
  amount_lcy TEXT,
  currency_code TEXT
);

-- Table: fact_cash_balance
CREATE TABLE IF NOT EXISTS staging.fact_cash_balance (
  date_id TEXT,
  cash_in TEXT,
  cash_out TEXT,
  cash_balance TEXT NOT NULL
);