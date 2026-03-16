-- This model creates a dimension table for customers, ensuring that each customer_id is unique and associated with a single country.
--  It uses the ROW_NUMBER() window function to select the first occurrence of each customer_id based on the invoice_date, which helps to resolve any potential duplicates in the source data.
-- when dbt runs, it will create a table in the "gold" schema defined in profile.yml file. tables store the transformed data and are optimized for querying and analysis.
{{ config(materialized='table', schema='gold') }}

WITH source AS (
    SELECT * FROM {{ ref('silver_ecommerce') }}
)

SELECT
    customer_id,
    country
FROM source
WHERE customer_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY invoice_date
) = 1 -- keep only the first occurrence of each customer_id based on invoice_date, resolving duplicates

