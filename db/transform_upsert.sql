-- Table: dim_product
WITH cleaned AS (
  SELECT
    product_id,
    product_name,
    product_type,
    currency,
    NULLIF(price_monthly, '')::NUMERIC(18,2) AS price_monthly,
    NULLIF(price_annual, '')::NUMERIC(18,2) AS price_annual
  FROM staging.dim_product
)
INSERT INTO core.dim_product AS t (
  product_id,
  product_name,
  product_type,
  currency,
  price_monthly,
  price_annual
)
SELECT * FROM cleaned
ON CONFLICT (product_id) DO UPDATE
SET product_name = EXCLUDED.product_name,
    product_type = EXCLUDED.product_type,
    currency = EXCLUDED.currency,
    price_monthly = EXCLUDED.price_monthly,
    price_annual = EXCLUDED.price_annual;

-- Table: dim_customer
WITH cleaned AS (
  SELECT
    customer_id,
    name,
    email,
    country,
    region,
    TO_DATE(NULLIF(signup_date, ''), 'YYYY-MM-DD') AS signup_date,
    CASE WHEN LOWER(is_active) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
         WHEN LOWER(is_active) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
         ELSE NULL
    END AS is_active
  FROM staging.dim_customer
)
INSERT INTO core.dim_customer AS t(
  customer_id, 
  name, 
  email, 
  country, 
  region, 
  signup_date, 
  is_active
)
SELECT * FROM cleaned
ON CONFLICT (customer_id) DO UPDATE
SET name = EXCLUDED.name,
    email = EXCLUDED.email,
    country = EXCLUDED.country,
    region = EXCLUDED.region,
    signup_date = EXCLUDED.signup_date,
    is_active = EXCLUDED.is_active;

-- Table: dim_date
WITH cleaned AS (
  SELECT
    TO_DATE(NULLIF(date_id, ''), 'YYYYMMDD') AS date_id,
    TO_DATE(NULLIF(date, ''), 'YYYY-MM-DD') AS date,
    NULLIF(year, '')::INT AS year,
    NULLIF(quarter, '')::INT AS quarter,
    NULLIF(month, '')::INT AS month,
    NULLIF(day, '')::INT AS day
  FROM staging.dim_date
)
INSERT INTO core.dim_date AS t (date_id, date, year, quarter, month, day)
SELECT * FROM cleaned
ON CONFLICT (date_id) DO UPDATE
SET date = EXCLUDED.date,
    year = EXCLUDED.year,
    quarter = EXCLUDED.quarter,
    month = EXCLUDED.month,
    day = EXCLUDED.day;

-- Table: dim_currency
WITH cleaned AS (
  SELECT
    currency_code,
    currency_name
  FROM staging.dim_currency
)
INSERT INTO core.dim_currency AS t (currency_code, currency_name)
SELECT * FROM cleaned
ON CONFLICT (currency_code) DO UPDATE
SET currency_name = EXCLUDED.currency_name;

-- Table: dim_department
WITH cleaned AS (
  SELECT
    department_id,
    department_name
  FROM staging.dim_department
)
INSERT INTO core.dim_department AS t (department_id, department_name)
SELECT * FROM cleaned
ON CONFLICT (department_id) DO UPDATE
SET department_name = EXCLUDED.department_name;

-- Table: dim_employee
WITH cleaned AS (
  SELECT
    employee_id,
    employee_name,
    TO_DATE(NULLIF(hire_date, ''), 'YYYYMMDD') AS hire_date,
    TO_DATE(NULLIF(termination_date, ''), 'YYYYMMDD') AS termination_date,
    department_id,
    country,
    title,
    NULLIF(salary, '')::NUMERIC(18,2) AS salary
  FROM staging.dim_employee
)
INSERT INTO core.dim_employee AS t (
  employee_id, 
  employee_name, 
  hire_date, 
  termination_date, 
  department_id, 
  country, 
  title, 
  salary
)
SELECT * FROM cleaned
ON CONFLICT (employee_id) DO UPDATE
SET employee_name = EXCLUDED.employee_name,
    hire_date = EXCLUDED.hire_date,
    termination_date = EXCLUDED.termination_date,
    department_id = EXCLUDED.department_id,
    country = EXCLUDED.country,
    title = EXCLUDED.title,
    salary = EXCLUDED.salary;

-- Table: dim_other_expense_type
WITH cleaned AS (
  SELECT
    other_expense_type_id,
    expense_type_name,
    expense_description
  FROM staging.dim_other_expense_type
)
INSERT INTO core.dim_other_expense_type AS t (
  other_expense_type_id, 
  expense_type_name, 
  expense_description
)
SELECT * FROM cleaned
ON CONFLICT (other_expense_type_id) DO UPDATE
SET expense_type_name = EXCLUDED.expense_type_name,
    expense_description = EXCLUDED.expense_description;

-- Table: fact_fx_rate
WITH cleaned AS (
  SELECT
    TO_DATE(NULLIF(date_id, ''), 'YYYYMMDD') AS date_id,
    currency_code,
    NULLIF(rate_to_usd, '')::NUMERIC(18,6) AS rate_to_usd
  FROM staging.fact_fx_rate
)
INSERT INTO core.fact_fx_rate AS t (date_id, currency_code, rate_to_usd)
SELECT * FROM cleaned
ON CONFLICT (date_id, currency_code) DO UPDATE
SET rate_to_usd = EXCLUDED.rate_to_usd;

-- Table: fact_subscription_revenue
-- one-time DDL to create constraint on source_system and source_record_id
CREATE UNIQUE INDEX IF NOT EXISTS uq_subrev_source
  ON core.fact_subscription_revenue (source_system, source_record_id);

-- load
WITH cleaned AS (
  SELECT
    source_system,
    source_record_id,
    TO_DATE(NULLIF(date_id, ''), 'YYYYMMDD') AS date_id,
    customer_id,
    product_id,
    billing_cycle,
    NULLIF(amount_lcy, '')::NUMERIC(18,2) AS amount_lcy,
    currency_code,
    country,
    ingest_batch_id
  FROM staging.fact_subscription_revenue
)
INSERT INTO core.fact_subscription_revenue AS t (
  source_system,
  source_record_id,
  date_id,
  customer_id,
  product_id,
  billing_cycle,
  amount_lcy,
  currency_code,
  country,
  ingest_batch_id
)
SELECT 
  source_system,
  source_record_id,
  date_id,
  customer_id,
  product_id,
  billing_cycle,
  amount_lcy,
  currency_code,
  country,
  ingest_batch_id 
FROM cleaned
ON CONFLICT (source_system, source_record_id) DO NOTHING;


-- Table: fact_cloud_cost
WITH cleaned AS (
  SELECT
    cloud_cost_id,
    TO_DATE(NULLIF(date_id, ''), 'YYYYMMDD') AS date_id,
    provider_name,
    NULLIF(amount_lcy, '')::NUMERIC(18,2) AS amount_lcy,
    currency_code
  FROM staging.fact_cloud_cost
)
INSERT INTO core.fact_cloud_cost AS t (
  cloud_cost_id, 
  date_id, 
  provider_name, 
  amount_lcy, 
  currency_code
)
SELECT * FROM cleaned
ON CONFLICT (cloud_cost_id) DO UPDATE
SET date_id = EXCLUDED.date_id,
    provider_name = EXCLUDED.provider_name,
    amount_lcy = EXCLUDED.amount_lcy,
    currency_code = EXCLUDED.currency_code;

-- Table: fact_payment_processor_fees
-- one-time DDL to create constraint on source_system and source_record_id
CREATE UNIQUE INDEX IF NOT EXISTS uq_ppc_tx
  ON core.fact_payment_processing_cost (transaction_id);

-- one-time DDL for lineage (needed for ON CONFLICT)
ALTER TABLE core.fact_payment_processing_cost
  ADD CONSTRAINT uq_ppc_source UNIQUE (source_system, source_record_id);

-- load
WITH ins_sub AS (
  SELECT fact_id, source_system, source_record_id, date_id, currency_code
  FROM core.fact_subscription_revenue
),
cleaned AS (
  SELECT
    p.source_system,
    p.sub_source_record_id,
    p.source_record_id,
    TO_DATE(NULLIF(p.date_id, ''), 'YYYYMMDD') AS date_id,
    p.processor_name,
    NULLIF(p.amount_lcy, '')::NUMERIC(18,2) AS amount_lcy,
    p.currency_code
  FROM staging.fact_payment_processing_cost p
),
joined AS (
  SELECT
    c.source_system,
    c.sub_source_record_id,
    c.source_record_id,
    c.date_id,
    c.processor_name,
    s.fact_id AS transaction_id,
    c.amount_lcy,
    c.currency_code
  FROM cleaned c
  JOIN ins_sub s
    ON s.source_system = c.source_system
   AND s.source_record_id = c.sub_source_record_id
)
INSERT INTO core.fact_payment_processing_cost AS t (
  source_system,
  sub_source_record_id,
  source_record_id,
  date_id,
  processor_name,
  transaction_id,
  amount_lcy,
  currency_code,
  ingest_batch_id
)
SELECT
  j.source_system,
  j.sub_source_record_id,
  j.source_record_id,
  j.date_id,
  j.processor_name,
  j.transaction_id,
  j.amount_lcy,
  j.currency_code,
  'ppc_batch_1'
FROM joined AS j
ON CONFLICT (source_system, source_record_id) DO NOTHING;


-- Table: fact_other_expenses
WITH cleaned AS (
  SELECT
    other_expense_id,
    TO_DATE(NULLIF(date_id, ''), 'YYYYMMDD') AS date_id,
    other_expense_type_id,
    vendor_name,
    invoice_number,
    NULLIF(amount_lcy, '')::NUMERIC(18,2) AS amount_lcy,
    currency_code
  FROM staging.fact_other_expenses
)
INSERT INTO core.fact_other_expenses AS t (
  other_expense_id,
  date_id,
  other_expense_type_id,
  vendor_name,
  invoice_number,
  amount_lcy,
  currency_code
)
SELECT * FROM cleaned
ON CONFLICT (other_expense_id) DO UPDATE
SET date_id = EXCLUDED.date_id,
    other_expense_type_id = EXCLUDED.other_expense_type_id,
    vendor_name = EXCLUDED.vendor_name,
    invoice_number = EXCLUDED.invoice_number,
    amount_lcy = EXCLUDED.amount_lcy,
    currency_code = EXCLUDED.currency_code;

-- Table: fact_marketing_spend
WITH cleaned AS (
  SELECT
    marketing_spend_id,
    TO_DATE(NULLIF(date_id, ''), 'YYYYMMDD') AS date_id,
    channel,
    campaign_id,
    NULLIF(amount_lcy, '')::NUMERIC(18,2) AS amount_lcy,
    currency_code
  FROM staging.fact_marketing_spend
)
INSERT INTO core.fact_marketing_spend AS t (
  marketing_spend_id,
  date_id,
  channel,
  campaign_id,
  amount_lcy,
  currency_code
)
SELECT * FROM cleaned
ON CONFLICT (marketing_spend_id) DO UPDATE
SET date_id = EXCLUDED.date_id,
    channel = EXCLUDED.channel,
    campaign_id = EXCLUDED.campaign_id,
    amount_lcy = EXCLUDED.amount_lcy,
    currency_code = EXCLUDED.currency_code;


-- Table: fact_subscription (derived from fact_subscription_revenue)
TRUNCATE TABLE core.fact_subscription CASCADE;
WITH ev AS (
  SELECT
    date_id AS event_date,
    customer_id,
    product_id,
    LOWER(billing_cycle) AS billing_cycle,
    amount_lcy::NUMERIC(18,2) AS price_per_period,
    currency_code
  FROM core.fact_subscription_revenue
),
ev_norm AS (
  SELECT e.*,
    CASE
      WHEN billing_cycle = 'annual' THEN price_per_period / 12.0
      WHEN billing_cycle = 'monthly' THEN price_per_period
      ELSE 0.0
    END AS mrr_value
  FROM ev e
),
ordered AS (
  SELECT *,
    LAG(event_date) OVER (PARTITION BY customer_id, product_id ORDER BY event_date) AS prev_date,
    LAG(billing_cycle) OVER (PARTITION BY customer_id, product_id ORDER BY event_date) AS prev_cycle,
    LAG(price_per_period) OVER (PARTITION BY customer_id, product_id ORDER BY event_date) AS prev_price
  FROM ev_norm
),
with_thresholds AS (
  SELECT *,
    CASE billing_cycle
      WHEN 'monthly' THEN INTERVAL '30 days'
      WHEN 'annual' THEN INTERVAL '365 days'
    END AS expected_interval,
    INTERVAL '45 days' AS grace
  FROM ordered
),
flags AS (
  SELECT *,
    CASE
      WHEN prev_date IS NULL THEN 1
      WHEN billing_cycle <> prev_cycle THEN 1
      WHEN price_per_period <> prev_price THEN 1
      WHEN (event_date - prev_date) > EXTRACT(DAY FROM (expected_interval + grace)) THEN 1
      ELSE 0
    END AS is_new_run
  FROM with_thresholds
),
runs AS (
  SELECT *,
    SUM(is_new_run) OVER (
      PARTITION BY customer_id, product_id
      ORDER BY event_date
    ) AS run_id
  FROM flags
),
agg AS (
  SELECT
    customer_id, product_id, billing_cycle, currency_code,
    MAX(price_per_period) AS price_per_period,
    MAX(mrr_value) AS mrr_value,
    MIN(event_date) AS start_date,
    MAX(event_date) AS last_event_date,
    run_id
  FROM runs
  GROUP BY customer_id, product_id, billing_cycle, currency_code, run_id
),
asof AS (
  SELECT MAX(date_id) AS asof_date
  FROM core.dim_date
),
end_calc AS (
  SELECT a.*,
    CASE billing_cycle
      WHEN 'monthly' THEN last_event_date + INTERVAL '30 days'
      WHEN 'annual' THEN last_event_date + INTERVAL '365 days'
    END AS expected_renewal,
    INTERVAL '45 days' AS grace,
    (SELECT asof_date FROM asof) AS asof_date
  FROM agg a
)
INSERT INTO core.fact_subscription (
  customer_id,
  product_id,
  billing_cycle,
  price_per_period,
  mrr_value,
  currency_code,
  start_date,
  end_date,
  status
)
SELECT
  customer_id,
  product_id,
  billing_cycle,
  price_per_period,
  mrr_value,
  currency_code,
  start_date,
  CASE WHEN asof_date > (expected_renewal + grace)
        THEN (expected_renewal - INTERVAL '1 day')::DATE
        ELSE NULL::date END AS end_date,
  CASE WHEN asof_date > (expected_renewal + grace)
        THEN 'ended' 
        ELSE 'active' END AS status
FROM end_calc;


-- Table: fact_subscription_snapshot_monthly
TRUNCATE TABLE core.fact_subscription_snapshot_monthly;
WITH months AS (
	SELECT DISTINCT DATE_TRUNC('month', date_id)::DATE AS month_start
	FROM core.dim_date
	ORDER BY 1
),
subs AS (
	SELECT * FROM core.fact_subscription
),
active AS (
	SELECT 
		m.month_start AS snapshot_month,
		s.subscription_id,
		s.customer_id,
		s.product_id,
		s.mrr_value
	FROM months m
	JOIN subs s
	ON s.start_date::DATE <= (m.month_start + INTERVAL '1 month - 1 day')::DATE
	AND (s.end_date::DATE IS NULL OR s.end_date::DATE >= m.month_start) 
)
INSERT INTO core.fact_subscription_snapshot_monthly
  (snapshot_month, subscription_id, customer_id, product_id, mrr_value)
SELECT * FROM active
ON CONFLICT (snapshot_month, subscription_id) DO UPDATE
SET mrr_value = EXCLUDED.mrr_value;