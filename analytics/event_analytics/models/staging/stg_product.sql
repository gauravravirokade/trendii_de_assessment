{{ config(
    materialized='view'
) }}

select * from {{ ref('src_product') }}