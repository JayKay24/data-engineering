WITH enriched AS (
    SELECT * FROM {{ ref('int_enriched_purchases') }}
)

SELECT
    category,
    purchase_quarter,
    ROUND(SUM(purchase_amount), 2) AS total_revenue,
    COUNT(purchase_id) AS units_sold
FROM enriched
GROUP BY
    category,
    purchase_quarter
