WITH
    base_product_impressions AS (
        SELECT
            *
        FROM
            {{ ref('src_events') }}
        WHERE
            event_name = 'ProductImpressions'
    ),

    base_products AS (
        SELECT
            *
        FROM
            {{ ref('src_product') }}
    ),

    flattened AS (
        SELECT
            event_created_at
          , event_id
          , publisher_id
          , domain
          , url
          , device_id
          , user_agent
          , page_view_id
          , event_data_json
          , JSONB_ARRAY_ELEMENTS(event_data_json -> 'products') AS product_json
        FROM
            base_product_impressions
    ),

    extract_products AS (
        SELECT
            event_created_at
          , event_id
          , publisher_id
          , domain
          , url
          , device_id
          , user_agent
          , page_view_id
          , event_data_json
          , event_data_json ->> 'image_id' AS image_id
          , product_json ->> 'product_id' AS product_id
          , product_json ->> 'brand_id' AS brand_id
          , product_json ->> 'product_url' AS product_url
          , product_json ->> 'product_name' AS product_name
          , (product_json ->> 'product_price')::FLOAT AS product_price
          , product_json ->> 'product_image_url' AS product_image_url
        FROM
            flattened
    ),

    flag_products AS (
        SELECT
            ep.*
          , CASE
                WHEN (bs.product_id IS NULL AND bs.product_brand_id IS NULL)
                    THEN 0
                ELSE 1 END AS is_product_brand_in_dim
        FROM
            extract_products AS ep
        LEFT JOIN
            base_products AS bs
            ON ep.product_id = bs.product_id
            AND ep.brand_id = bs.product_brand_id
    ),

    final_dedupe AS (
        SELECT
            *
          , ROW_NUMBER() OVER (
                PARTITION BY event_id, product_id
                ORDER BY event_created_at DESC
            ) AS row_num
        FROM
            flag_products
    )

SELECT
    md5(cast(coalesce(cast(event_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(product_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS impression_surrogate_key
  , event_id
  , publisher_id
  , domain
  , url
  , device_id
  , user_agent
  , page_view_id
  , image_id
  , product_id
  , brand_id
  , product_url
  , product_name
  , product_price
  , product_image_url
  , is_product_brand_in_dim
  , event_created_at
FROM
    final_dedupe
WHERE
    row_num = 1