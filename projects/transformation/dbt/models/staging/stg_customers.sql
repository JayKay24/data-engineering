WITH raw_customers AS (
    SELECT * FROM read_json_auto('data/raw/customers.json')
)

SELECT
    customer_id,
    segment,
    region,
    CAST(registration_date AS DATE) AS registration_date
FROM raw_customers
WHERE customer_id IS NOT NULL
