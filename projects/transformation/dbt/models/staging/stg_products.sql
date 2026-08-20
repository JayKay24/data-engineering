WITH raw_products AS (
    SELECT * FROM read_json_auto('data/raw/products.json')
)

SELECT
    product_id,
    category,
    CAST(price AS DOUBLE) AS price,
    brand
FROM raw_products
WHERE product_id IS NOT NULL
