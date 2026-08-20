WITH purchases AS (
    SELECT * FROM {{ ref('stg_purchases') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    p.purchase_id,
    p.customer_id,
    p.product_id,
    p.purchase_amount,
    p.purchase_timestamp,
    p.purchase_date,
    p.purchase_quarter,
    p.payment_method,
    c.segment,
    c.region,
    c.registration_date,
    pr.category,
    pr.price AS product_catalog_price,
    pr.brand
FROM purchases p
LEFT JOIN customers c ON p.customer_id = c.customer_id
LEFT JOIN products pr ON p.product_id = pr.product_id
