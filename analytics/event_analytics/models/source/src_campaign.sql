SELECT
    *
FROM
    {{ source('raw', 'dim_campaign') }}