-- This model creates a dimension table for products, ensuring that each stock_code is unique and associated with a single description and unit_price.
-- It uses the ROW_NUMBER() window function to select the first occurrence of each stock_code based on the invoice_date, which helps to resolve any potential duplicates in the source data.
-- when dbt runs, it will create a table in the "gold" schema defined in profile.yml file.
-- tables store the transformed data and are optimized for querying and analysis.

-- it includes the following columns:
-- stock_code   → natural key
-- description  → product name
-- unit_price   → price at first appearance (SCD Type 0)

{{ config(materialized='table', schema='gold') }}

WITH source AS (
    SELECT * FROM {{ ref('silver_ecommerce') }}
)

SELECT
    UPPER(TRIM(stock_code))     AS stock_code,      -- normalise casing and whitespace to prevent duplicates
    description,
    unit_price
FROM source
WHERE stock_code IS NOT NULL
AND UPPER(TRIM(stock_code)) NOT IN (
    'B',            -- Adjust bad debt
    'M',            -- Manual
    'POST',         -- Postage
    'DOT',          -- Dotcom Postage
    'BANK CHARGES'  -- Bank Charges
)                                                   -- exclude non-product administrative entries
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY UPPER(TRIM(stock_code))            -- partition on normalised key to correctly deduplicate
    ORDER BY invoice_date
) = 1   -- keep only the first occurrence of each stock_code based on invoice_date, resolving duplicates



