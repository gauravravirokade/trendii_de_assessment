{{ config(MATERIALIZED='table') }}

WITH
    base AS (SELECT
                 event_name
               , COUNT(*)                                                                                        AS total_events
               , COUNT(event_data_json ->> 'mounts')
                 FILTER (WHERE event_data_json ->> 'mounts' IS NOT NULL)                                              AS mounts_not_null
               , COUNT(event_data_json ->> 'products')
                 FILTER (WHERE event_data_json ->> 'products' IS NOT NULL)                                            AS products_not_null
               , COUNT(event_data_json ->> 'brand_id')
                 FILTER (WHERE event_data_json ->> 'brand_id' IS NOT NULL)                                            AS brand_id_not_null
               , COUNT(event_data_json ->> 'click_id')
                 FILTER (WHERE event_data_json ->> 'click_id' IS NOT NULL)                                            AS click_id_not_null
               , COUNT(event_data_json ->> 'image_id')
                 FILTER (WHERE event_data_json ->> 'image_id' IS NOT NULL)                                            AS image_id_not_null
               , COUNT(event_data_json ->> 'product_id')
                 FILTER (WHERE event_data_json ->> 'product_id' IS NOT NULL)                                          AS product_id_not_null
               , COUNT(event_data_json ->> 'product_url')
                 FILTER (WHERE event_data_json ->> 'product_url' IS NOT NULL)                                         AS product_url_not_null
               , COUNT(event_data_json ->> 'product_name')
                 FILTER (WHERE event_data_json ->> 'product_name' IS NOT NULL)                                        AS product_name_not_null
               , COUNT(event_data_json ->> 'product_price')
                 FILTER (WHERE event_data_json ->> 'product_price' IS NOT NULL)                                       AS product_price_not_null
               , COUNT(event_data_json ->> 'product_image_url')
                 FILTER (WHERE event_data_json ->> 'product_image_url' IS NOT NULL)                                   AS product_image_url_not_null
             FROM
                 {{ ref('src_events') }}
             GROUP BY
                 event_name)

  , flags AS (SELECT
                  *
                  -- TagLoaded: no populated fields
                , CASE
                      WHEN event_name = 'TagLoaded' AND (
                          mounts_not_null = 0 AND products_not_null = 0 AND brand_id_not_null = 0
                              AND click_id_not_null = 0 AND image_id_not_null = 0 AND product_id_not_null = 0
                              AND product_url_not_null = 0 AND product_name_not_null = 0
                              AND product_price_not_null = 0 AND product_image_url_not_null = 0
                          )
                          THEN 1
                      WHEN event_name = 'TagLoaded'
                          THEN 0
                      ELSE 1
                      END AS is_tagloaded_data_valid

                  -- Mounts: only mounts populated
                , CASE
                      WHEN event_name = 'Mounts' AND (
                          mounts_not_null > 0 AND products_not_null = 0 AND brand_id_not_null = 0
                              AND click_id_not_null = 0 AND image_id_not_null = 0
                              AND product_id_not_null = 0 AND product_url_not_null = 0
                              AND product_name_not_null = 0 AND product_price_not_null = 0
                              AND product_image_url_not_null = 0
                          )
                          THEN 1
                      WHEN event_name = 'Mounts'
                          THEN 0
                      ELSE 1
                      END AS is_mounts_data_valid

                  -- ProductClick: only single product fields populated
                , CASE
                      WHEN event_name = 'ProductClick' AND (
                          products_not_null = 0 AND brand_id_not_null > 0 AND click_id_not_null > 0
                              AND image_id_not_null > 0 AND product_id_not_null > 0
                              AND product_url_not_null > 0 AND product_name_not_null > 0
                              AND product_price_not_null > 0 AND product_image_url_not_null > 0
                          )
                          THEN 1
                      WHEN event_name = 'ProductClick'
                          THEN 0
                      ELSE 1
                      END AS is_productclick_data_valid

                  -- ProductImpressions: only products + image_id populated
                , CASE
                      WHEN event_name = 'ProductImpressions' AND (
                          products_not_null > 0 AND image_id_not_null > 0
                              AND brand_id_not_null = 0 AND click_id_not_null = 0
                              AND product_id_not_null = 0 AND product_url_not_null = 0
                              AND product_name_not_null = 0 AND product_price_not_null = 0
                              AND product_image_url_not_null = 0
                          )
                          THEN 1
                      WHEN event_name = 'ProductImpressions'
                          THEN 0
                      ELSE 1
                      END AS is_productimpressions_data_valid
              FROM
                  base)

SELECT
    *
FROM
    flags
