SELECT
    COUNT(DISTINCT device_id) AS unique_users_advertised_too
FROM
    {{ ref('stg_event_product_impressions') }}
--     trendii_de_assessment.grr_dev.stg_event_product_impressions;