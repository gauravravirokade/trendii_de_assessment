-- This query provides the list of products missing from the dim_product source.
SELECT DISTINCT
    product_id
  , product_name
  , brand_id
FROM
    {{ ref('stg_event_product_impressions') }}
--     grr_dev.stg_event_product_impressions
WHERE
    is_product_brand_in_dim = 0
-- GROUP BY
--     product_id, product_name, brand_id
