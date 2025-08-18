WITH
    base_impressions
        AS (SELECT
                product_id
              , brand_id
              , event_created_at
            FROM
                {{ ref('stg_event_product_impressions') }}
--         grr_dev.stg_event_product_impressions
    )

  , base_campaign
        AS (SELECT
                campaign_id
              , campaign_name
              , campaign_brand_id
              , campaign_valid_from
              , campaign_valid_to
            FROM
                {{ ref('stg_campaign') }}
--     trendii_de_assessment.grr_dev.stg_campaign
    )

  , campaign_impression_attribution
        AS (SELECT
                bi.product_id
              , bc.campaign_id
              , bc.campaign_name
--     *
            FROM
                base_campaign AS bc
                    LEFT JOIN
                    base_impressions AS bi
                    ON bc.campaign_brand_id = bi.brand_id
                        AND
                       bi.event_created_at BETWEEN bc.campaign_valid_from AND bc.campaign_valid_to)


--    select * from campaign_impression_attribution where campaign_id = '16962092580';
  , campaign_impression_counts
        AS (SELECT
                campaign_id
              , campaign_name
              , product_id
              , COUNT(product_id) AS total_impression_count
            FROM
                campaign_impression_attribution
            GROUP BY
                campaign_id
              , campaign_name
              , product_id)

  , ranked_impressions
        AS (SELECT
                *
              , ROW_NUMBER() OVER (
            PARTITION BY campaign_id
            ORDER BY total_impression_count DESC
            ) AS rank_in_campaign
            FROM
                campaign_impression_counts)

  , final
        AS (SELECT
                campaign_id
              , campaign_name
              , product_id
              , total_impression_count
            FROM
                ranked_impressions
            WHERE
                rank_in_campaign = 1)

SELECT
    *
FROM
    final