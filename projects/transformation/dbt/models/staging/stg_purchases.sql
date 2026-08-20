WITH raw_purchases AS (
    SELECT * FROM read_json_auto('data/raw/purchases.json')
)

SELECT
    purchase_id,
    customer_id,
    product_id,
    CAST(purchase_amount AS DOUBLE) AS purchase_amount,
    CAST(timestamp AS TIMESTAMP) AS purchase_timestamp,
    CAST(timestamp AS DATE) AS purchase_date,
    EXTRACT(QUARTER FROM CAST(timestamp AS TIMESTAMP)) AS purchase_quarter,
    payment_method
FROM raw_purchases
WHERE
    purchase_amount IS NOT NULL
    AND product_id IS NOT NULL
    AND customer_id IS NOT NULL
