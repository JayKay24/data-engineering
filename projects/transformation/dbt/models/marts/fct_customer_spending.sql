WITH enriched AS (
    SELECT * FROM {{ ref('int_enriched_purchases') }}
)

SELECT
    customer_id,
    segment,
    region,
    purchase_quarter,
    ROUND(SUM(purchase_amount), 2) AS total_spent,
    COUNT(purchase_id) AS transaction_count
FROM enriched
GROUP BY
    customer_id,
    segment,
    region,
    purchase_quarter
