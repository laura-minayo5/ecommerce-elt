-- This model creates a date dimension table using the dbt_date package. 
-- The date range is from December 1, 2010 to December 9, 2011. 
-- The resulting table will include various date attributes such as year, month, day, quarter, etc., 
-- which can be used for time-based analysis in the e-commerce data warehouse.

-- Jinja function call to configure the model to be materialized as a table in the 'gold' schema.
{{ config(materialized='table', schema='gold') }}


-- Using a Common Table Expression (CTE) to generate a date spine, which is a continuous sequence of dates.
-- The dbt_utils.date_spine jinja macro call is used to create this sequence, specifying the date part as 'day' and the start and end dates.
-- The date_spine CTE will produce a table with a single column 'date_day' that contains all the dates in the specified range.

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-12-01' as date)",
        end_date="cast('2011-12-10' as date)"
    ) }}
)

SELECT
    date_day                                              AS date,
    TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD'))             AS date_id,
    YEAR(date_day)                                        AS year,
    QUARTER(date_day)                                     AS quarter,
    MONTH(date_day)                                       AS month,
    TO_CHAR(date_day, 'MMMM')                            AS month_name,
    WEEKOFYEAR(date_day)                                  AS week,
    DAYOFWEEK(date_day)                                   AS day_of_week,
    TO_CHAR(date_day, 'DY')                              AS day_name,
    CASE WHEN DAYOFWEEK(date_day) IN (0, 6)
         THEN TRUE ELSE FALSE END                         AS is_weekend
FROM date_spine

