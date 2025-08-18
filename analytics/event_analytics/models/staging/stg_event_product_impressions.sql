{{ config(
    MATERIALIZED='table'
) }}

WITH
    base AS (SELECT
                 *
             FROM
                 {{ ref('src_events') }}
             --     grr_dev.src_events
--     raw.raw_events
             WHERE
                 event_name = 'ProductImpressions')

  , flattened AS (SELECT
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
                      base)

  , extract_products AS (SELECT
                             event_created_at
                           , event_id
                           , publisher_id
                           , domain
                           , url
                           , device_id
                           , user_agent
                           , page_view_id
                           , event_data_json
                           , event_data_json ->> 'image_id'            AS image_id
                           , product_json ->> 'product_id'             AS product_id
                           , product_json ->> 'brand_id'               AS brand_id
                           , product_json ->> 'product_url'            AS product_url
                           , product_json ->> 'product_name'           AS product_name
                           , (product_json ->> 'product_price')::FLOAT AS product_price
                           , product_json ->> 'product_image_url'      AS product_image_url
                         FROM
                             flattened)
  , final AS (SELECT
                  *
                , ROW_NUMBER() OVER (PARTITION BY event_id,product_id, brand_id ORDER BY event_created_at) AS row_num
              FROM
                  extract_products)

SELECT
    *
FROM
    final
WHERE
    row_num = 1
-- where event_id = 'e08927e3-61ac-4cdc-bda6-bf06ea03b5cb'
