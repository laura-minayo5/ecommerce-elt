{{ config(materialized='table', schema='marts') }}


-- Step 1: Get all sales transactions from the Gold fact table
-- Join to dim_date to get a proper date column (order_date) from the date_id integer key
-- This allows us to calculate recency based on actual dates instead of just date_id integers
WITH fact AS (
    SELECT
        f.*,
        d.date      AS order_date
    FROM {{ ref('fact_sales') }} f
    LEFT JOIN {{ ref('dim_date') }} d ON f.date_id = d.date_id
),

-- Step 2: Create a spine of all months in the data
-- DATE_TRUNC on order_date (from dim_date join) chops off the day and time, leaving only the first day of the month. So:
-- 2011-03-15  →  2011-03-01
-- 2011-03-31  →  2011-03-01
-- DISTINCT ensures we only get one row per month, even if there are many transactions in that month
--  we need this to get one RFM score per customer per month

months AS (
    SELECT DISTINCT
        DATE_TRUNC('month', order_date)     AS snapshot_month
    FROM fact
    WHERE customer_id IS NOT NULL
),

-- Step 3: Calculate RECENCY and FREQUENCY per customer per month
-- Recency   → most recent order date up to and including that month (cumulative)
-- Frequency → distinct invoices placed up to and including that month (cumulative)
-- Cumulative so that a customer's history builds over time, not just within the month
recency_frequency AS (
    SELECT
        f.customer_id,
        m.snapshot_month,
        MAX(f.date_id)                      AS most_recent_order,   -- most recent purchase up to this month
        COUNT(DISTINCT f.invoice_no)        AS number_of_orders     -- total orders up to this month
    FROM fact f
    JOIN months m
        ON DATE_TRUNC('month', f.order_date) <= m.snapshot_month    -- cumulative: all history up to this month
    WHERE f.customer_id IS NOT NULL
    GROUP BY f.customer_id, m.snapshot_month
),

-- Step 4: Calculate MONETARY per customer per month (cumulative)
-- Monetary → total amount spent across all orders
monetary AS (
    SELECT
        f.customer_id,
        m.snapshot_month,
        SUM(f.total_price)                  AS total_spent          -- total spend up to this month
    FROM fact f
    JOIN months m
        ON DATE_TRUNC('month', f.order_date) <= m.snapshot_month    -- cumulative spend
    WHERE f.customer_id IS NOT NULL              -- only include real customers, not nulls        
     AND f.total_price IS NOT NULL              -- exclude refunds or cancelled orders with null total_price
     AND f.total_price > 0                     -- exclude refunds or cancelled orders with zero or negative total_price
    GROUP BY f.customer_id, m.snapshot_month    -- group by customer and month to get one row per customer per month
),

-- Step 5: Join recency, frequency, monetary into one row per customer per month
rfm_base AS (
    SELECT
        rf.customer_id,
        rf.snapshot_month,
        rf.most_recent_order,
        rf.number_of_orders,
        m.total_spent
    FROM recency_frequency rf
    LEFT JOIN monetary m
        ON rf.customer_id = m.customer_id
        AND rf.snapshot_month = m.snapshot_month
),

-- Step 6: Score each customer 1-5 on each RFM dimension using NTILE(5)
-- NTILE(5) ranks all customers within each month (PARTITION BY snapshot_month), 
-- ordered oldest to newest (ORDER BY most_recent_order ASC), 
-- assigning bucket 1 to the oldest and bucket 5 to the most recent.
-- Scores are calculated WITHIN each snapshot_month so customers are ranked against peers that month
rfm_scored AS (
    SELECT
        customer_id,
        snapshot_month,
        most_recent_order,
        number_of_orders,
        total_spent,

        -- Recency: ASC so most recent customers get score 5
        NTILE(5) OVER (
            PARTITION BY snapshot_month
            ORDER BY most_recent_order ASC
        )                                   AS recency_score,

        -- Frequency: ASC so most frequent customers get score 5
        NTILE(5) OVER (
            PARTITION BY snapshot_month
            ORDER BY number_of_orders ASC
        )                                   AS frequency_score,

        -- Monetary: ASC so highest spenders get score 5
        NTILE(5) OVER (
            PARTITION BY snapshot_month
            ORDER BY total_spent ASC
        )                                   AS monetary_score

    FROM rfm_base
)

-- Step 7: Final output — add total RFM score and customer segment label
SELECT
    customer_id,
    snapshot_month,
    most_recent_order,
    number_of_orders,
    total_spent,
    recency_score,
    frequency_score,
    monetary_score,

    -- total score: sum of all three scores, range 3-15
    recency_score + frequency_score + monetary_score    AS rfm_total_score,

    -- segment: classify customer based on their RFM scores
    -- Customer segments: Champion, Loyal Customer, New Customer, At Risk, Lost, Potential Loyalist
    -- evaluated top to bottom — first matching condition wins
    CASE
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4
            THEN 'Champion'           -- bought recently, often, and spent a lot
        WHEN recency_score >= 3 AND frequency_score >= 3
            THEN 'Loyal Customer'     -- consistent buyers, decent recency
        WHEN recency_score >= 4 AND frequency_score <= 2
            THEN 'New Customer'       -- bought recently but not often yet
        WHEN recency_score <= 2 AND frequency_score >= 3
            THEN 'At Risk'            -- used to buy often but not recently
        WHEN recency_score <= 2 AND frequency_score <= 2
            THEN 'Lost'               -- haven't bought recently and rarely bought
        ELSE
            'Potential Loyalist'      -- everything else — decent scores, not yet loyal
    END                                                 AS customer_segment
FROM rfm_scored
