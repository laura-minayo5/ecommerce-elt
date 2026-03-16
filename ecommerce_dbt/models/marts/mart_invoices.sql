{{ config(materialized='table', schema='marts') }}

WITH fact AS (
    SELECT * FROM {{ ref('fact_sales') }}
),
customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),
products AS (
    SELECT * FROM {{ ref('dim_products') }}
),
dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),
countries AS (
    SELECT alpha2, en FROM {{ ref('countries') }}
),

mart AS (
    SELECT
        -- surrogate key
        f.sales_id,
        
        -- invoice info
        f.invoice_no,
        CASE
            WHEN f.customer_id IS NULL THEN 'Cancelled'
            ELSE 'Shipped'
        END                          AS status,

        -- product info
        f.stock_code,
        p.description,
        p.unit_price,

        -- quantities and revenue
        f.quantity,
        f.total_price,

        -- customer info
        f.customer_id,
        c.country,
        co.alpha2                    AS country_code,

        -- date info
        d.date                       AS invoice_date,  -- matches dim_date alias
        d.month_name,
        d.year,
        d.quarter

    FROM fact f
    LEFT JOIN customers c  ON f.customer_id = c.customer_id
    LEFT JOIN products p   ON f.stock_code  = p.stock_code
    LEFT JOIN dates d      ON f.date_id     = d.date_id
    LEFT JOIN countries co ON c.country     = co.en
)

SELECT * FROM mart


-- Expected columns
-- invoice_no, status, stock_code, description, unit_price,
-- quantity, total_price, customer_id, country, country_code,
-- invoice_date, month_name, year, quarter