{{ config(
    MATERIALIZED='table'
) }}

WITH
    base_events AS (
        SELECT
            *
        FROM
            {{ ref('src_events') }}
        WHERE
            event_name = 'ProductClick'
    ),

    base_products AS (
        SELECT
            product_id,
            product_brand_id
        FROM
            {{ ref('src_product') }}
    ),

    extracted_data AS (
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

          , event_data_json ->> 'brand_id'               AS brand_id
          , event_data_json ->> 'click_id'               AS click_id
          , event_data_json ->> 'image_id'               AS image_id
          , event_data_json ->> 'product_id'             AS product_id
          , event_data_json ->> 'product_url'            AS product_url
          , event_data_json ->> 'product_name'           AS product_name
          , (event_data_json ->> 'product_price')::FLOAT AS product_price
          , event_data_json ->> 'product_image_url'      AS product_image_url
        FROM
            base_events
    ),

    final AS (
        SELECT
            ed.*
          , CASE
                WHEN (bp.product_id IS NOT NULL AND bp.product_brand_id IS NOT NULL)
                    THEN TRUE
                ELSE FALSE
            END AS is_product_in_dim
        FROM
            extracted_data AS ed
        LEFT JOIN
            base_products AS bp
            ON ed.product_id = bp.product_id AND ed.brand_id = bp.product_brand_id
    )

SELECT
    *
FROM
    final