
{{ config(materialized='table', schema='gold') }}

WITH source AS (
    SELECT * FROM {{ ref('silver_ecommerce') }}
),

fact AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['invoice_no', 'stock_code']) }} AS sales_id,
        invoice_no,
        UPPER(TRIM(stock_code))                                              AS stock_code,  -- normalise casing to match dim_products
        customer_id,
        TO_NUMBER(TO_CHAR(invoice_date::DATE, 'YYYYMMDD'))                   AS date_id,
        quantity,
        unit_price,
        total_price
    FROM source
    WHERE UPPER(TRIM(stock_code)) NOT IN (
        'B',            -- Adjust bad debt
        'M',            -- Manual
        'POST',         -- Postage
        'DOT',          -- Dotcom Postage
        'BANK CHARGES'  -- Bank Charges
    )                                                                     -- exclude non-product administrative entries
    AND customer_id IS NOT NULL
)

SELECT * FROM fact

-- generate_surrogate_key(['invoice_no', 'stock_code'])
-- → dbt_utils macro that hashes invoice_no + stock_code into a unique MD5 key
-- → this is your fact table's primary key

-- TO_NUMBER(TO_CHAR(invoice_date::DATE, 'YYYYMMDD')) AS date_id
-- → converts invoice_date to YYYYMMDD integer to match dim_date.date_id
-- → this is how fact_sales joins to dim_date
