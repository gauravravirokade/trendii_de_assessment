SELECT
    *
FROM
    {{ source('raw', 'dim_product') }}