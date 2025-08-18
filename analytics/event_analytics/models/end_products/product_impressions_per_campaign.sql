WITH
    base_impressions
        AS (SELECT
                product_id
              , product_name
              , brand_id
              , event_created_at
              , is_product_brand_in_dim
            FROM
                {{ ref('stg_event_product_impressions') }})

  , base_campaign
        AS (SELECT
                campaign_id
              , campaign_name
              , campaign_brand_id
              , campaign_valid_from
              , campaign_valid_to
            FROM
                {{ ref('stg_campaign') }})

  , campaign_impression_attribution
        AS (SELECT
                bi.product_id
              , bi.product_name
              , bi.brand_id
              , bi.is_product_brand_in_dim
              , bc.campaign_id
              , bc.campaign_name
            FROM
                base_campaign AS bc
                    LEFT JOIN
                    base_impressions AS bi
                    ON bc.campaign_brand_id = bi.brand_id
                        AND bi.event_created_at BETWEEN bc.campaign_valid_from AND bc.campaign_valid_to)

  , campaign_impression_counts
        AS (SELECT
                campaign_id
              , campaign_name
              , product_id
              , product_name
              , brand_id
              , is_product_brand_in_dim
              , COUNT(product_id) AS total_impression_count
            FROM
                campaign_impression_attribution
            GROUP BY
                campaign_id
              , campaign_name
              , product_id
              , product_name
              , brand_id
              , is_product_brand_in_dim)

  , ranked_impressions
        AS (SELECT
                *
              , ROW_NUMBER() OVER (
            PARTITION BY campaign_id
            ORDER BY total_impression_count DESC
            ) AS rank_in_campaign
            FROM
                campaign_impression_counts)
SELECT
    campaign_id
  , campaign_name
  , product_id
  , product_name
  , is_product_brand_in_dim
  , total_impression_count
FROM
    ranked_impressions
WHERE
    rank_in_campaign = 1