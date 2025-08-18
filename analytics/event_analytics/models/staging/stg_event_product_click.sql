{{ config(
    MATERIALIZED='table'
) }}

WITH
    base
        AS (SELECT
                *
            FROM
                {{ ref('src_events') }}
            WHERE
                event_name = 'ProductClick')

  , final
        AS (SELECT
                event_created_at
              , event_id
              , publisher_id
              , domain
              , url
              , device_id
              , user_agent
              , page_view_id
              , event_data_json

                -- product click fields (all top-level)
              , event_data_json ->> 'brand_id'               AS brand_id
              , event_data_json ->> 'click_id'               AS click_id
              , event_data_json ->> 'image_id'               AS image_id
              , event_data_json ->> 'product_id'             AS product_id
              , event_data_json ->> 'product_url'            AS product_url
              , event_data_json ->> 'product_name'           AS product_name
              , (event_data_json ->> 'product_price')::FLOAT AS product_price
              , event_data_json ->> 'product_image_url'      AS product_image_url
            FROM
                base)

SELECT
    *
FROM
    final
