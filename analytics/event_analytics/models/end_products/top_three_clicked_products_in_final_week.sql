WITH
    base
        AS (SELECT
                *
            FROM
--                 grr_dev.stg_event_product_click
{{ ref('stg_event_product_click') }})

  , max_date
        AS (SELECT
                MAX(event_created_at) AS latest_date
            FROM
                base)

  , final_week_clicks
        AS (SELECT
                c.product_id
              , c.brand_id
              , c.product_name
              , c.product_url
              , c.product_price
            FROM
                base AS c
                    CROSS JOIN
                    max_date AS md
            WHERE
                c.event_created_at >= md.latest_date - INTERVAL '7 days')

  , ranked_products
        AS (SELECT
                brand_id
              , product_id
              , product_name
              , COUNT(*)                                                         AS click_count
              , ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY COUNT(*) DESC) AS rank_in_brand
            FROM
                final_week_clicks
            GROUP BY
                brand_id, product_id, product_name)
  , final
        AS (SELECT
                brand_id
              , product_id
              , product_name
              , click_count
            FROM
                ranked_products
            WHERE
                rank_in_brand <= 3)

SELECT
    *
FROM
    final