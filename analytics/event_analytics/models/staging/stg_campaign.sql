{{ config(
    materialized='view'
) }}


SELECT
    *
FROM
    {{ ref('src_campaign') }}
--     grr_dev.src_campaign
WHERE
    campaign_current_record IS TRUE