{{ config(materialized='table', schema='marts') }}

-- Builds on top of mart_rfm_snapshot
-- Tracks how each customer's RFM scores and segment change month over month
-- Uses LAG to pull the previous month's values for comparison

WITH snapshot AS (
    SELECT * FROM {{ ref('mart_rfm_snapshot') }}
),

-- Step 1: Use LAG to bring in each customer's previous month values
-- PARTITION BY customer_id so LAG only looks at that customer's own history
-- ORDER BY snapshot_month so previous month is always the row directly before
rfm_with_previous AS (
    SELECT
        customer_id,
        snapshot_month,
        most_recent_order,
        number_of_orders,
        total_spent,
        recency_score,
        frequency_score,
        monetary_score,
        rfm_total_score,
        customer_segment,

        -- Previous month scores
        -- LAG looks back one row within each customer's own history (PARTITION BY customer_id), 
        -- ordered chronologically (ORDER BY snapshot_month), 
        -- and returns that previous month's recency score — so you can compare this month vs last month side by side.
        LAG(recency_score)      OVER (PARTITION BY customer_id ORDER BY snapshot_month) AS prev_recency_score,
        LAG(frequency_score)    OVER (PARTITION BY customer_id ORDER BY snapshot_month) AS prev_frequency_score,
        LAG(monetary_score)     OVER (PARTITION BY customer_id ORDER BY snapshot_month) AS prev_monetary_score,
        LAG(rfm_total_score)    OVER (PARTITION BY customer_id ORDER BY snapshot_month) AS prev_rfm_total_score,

        -- Previous month segment
        LAG(customer_segment)   OVER (PARTITION BY customer_id ORDER BY snapshot_month) AS prev_customer_segment,

        -- Previous month spend (for MoM monetary change)
        LAG(total_spent)        OVER (PARTITION BY customer_id ORDER BY snapshot_month) AS prev_total_spent

    FROM snapshot
)

-- Step 2: Final output — calculate deltas and classify the movement
SELECT
    customer_id,
    snapshot_month,

    -- Current scores
    recency_score,
    frequency_score,
    monetary_score,
    rfm_total_score,
    customer_segment,

    -- Previous month scores
    prev_recency_score,
    prev_frequency_score,
    prev_monetary_score,
    prev_rfm_total_score,
    prev_customer_segment,

    -- Score deltas (positive = improved, negative = declined)
    recency_score   - prev_recency_score    AS recency_score_change,
    frequency_score - prev_frequency_score  AS frequency_score_change,
    monetary_score  - prev_monetary_score   AS monetary_score_change,
    rfm_total_score - prev_rfm_total_score  AS total_score_change,

    -- Month over month spend change
    total_spent     - prev_total_spent      AS mom_spend_change,

    -- Segment movement: did the customer move up, down, or stay?
    CASE
        WHEN prev_customer_segment IS NULL
            THEN 'New'                      -- first time we see this customer
        WHEN customer_segment = prev_customer_segment
            THEN 'Stable'                   -- stayed in the same segment
        WHEN rfm_total_score > prev_rfm_total_score
            THEN 'Upgraded'                 -- moved to a better segment
        WHEN rfm_total_score < prev_rfm_total_score
            THEN 'Downgraded'               -- moved to a worse segment
        ELSE
            'Lateral'                       -- changed segment but same total score
    END                                     AS segment_movement,

    -- Explicit segment transition label for easy filtering/reporting
    --  handles the`NULL` returned by LAG for the very first month a customer appears
    -- so that we end up with New →  customer_segment instead of 'NULL' →  customer_segment
    -- e.g. 'Champion → At Risk' so stakeholders can see exact movements
    CASE
        WHEN prev_customer_segment IS NULL  THEN 'New → ' || customer_segment
        ELSE prev_customer_segment || ' → ' || customer_segment
    END                                     AS segment_transition

FROM rfm_with_previous
ORDER BY customer_id, snapshot_month

