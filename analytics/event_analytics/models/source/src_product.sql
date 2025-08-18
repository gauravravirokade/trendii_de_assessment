SELECT
    CAST(id AS VARCHAR)           AS product_id
  , CAST(brand_id AS VARCHAR)     AS product_brand_id
  , CAST(sku AS VARCHAR)          AS product_sku
  , CAST(name AS VARCHAR)         AS product_name
  , CAST(product_url AS VARCHAR)  AS product_url
  , CAST(image_url AS VARCHAR)    AS product_image_url
  , CAST(price AS FLOAT)          AS product_price
  , CAST(sale_price AS FLOAT)     AS product_sale_price
  , CAST(created_at AS TIMESTAMP) AS product_created_at
FROM
    {{ source('raw', 'dim_product') }}