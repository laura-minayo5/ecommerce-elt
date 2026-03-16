{{ config(materialized='view', schema='silver') }}
-- This model transforms Bronze raw data into cleaned transaction Silver layer of ecommerce data warehouse
-- When dbt runs, it will create a view in the "silver" schema  defined in profile.yml file
-- views store the transformation logic but do not store data, so they are faster to build and good for intermediate transformations and exploration. In the gold layer, we will create tables which store the transformed data and are optimized for querying and analysis.

-- selects all columns from the "raw_ecommerce" table in the "bronze" schema, which is defined as a source in dbt
WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_ecommerce') }}
),
-- the "cleaned" CTE performs data cleaning and transformation on the raw data
cleaned AS (
    SELECT
        InvoiceNo                                           AS invoice_no,
        StockCode                                           AS stock_code,
        TRIM(Description)                                   AS description,
        TRY_CAST(Quantity AS INT)                           AS quantity,
        TO_TIMESTAMP(InvoiceDate, 'MM/DD/YYYY HH24:MI')    AS invoice_date,
        TRY_CAST(UnitPrice AS FLOAT)                        AS unit_price,
        CustomerID                                          AS customer_id,
        TRIM(Country)                                       AS country,
        TRY_CAST(Quantity AS INT) * 
        TRY_CAST(UnitPrice AS FLOAT)                        AS total_price
    FROM source
    WHERE InvoiceNo IS NOT NULL
      AND StockCode IS NOT NULL
      AND TRY_CAST(Quantity AS INT) > 0
      AND TRY_CAST(UnitPrice AS FLOAT) > 0
),
-- the "deduplicated" CTE removes duplicate records based on invoice_no and stock_code, keeping only the first occurrence based on invoice_date
-- the ROW_NUMBER() function assigns a unique sequential integer to rows within a partition of a result set, and the QUALIFY clause filters the results to keep only the first occurrence of each invoice_no and stock_code combination
deduplicated AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY invoice_no, stock_code
        ORDER BY invoice_date
    ) = 1
)
SELECT * FROM deduplicated