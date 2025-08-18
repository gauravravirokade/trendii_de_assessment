{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref('src_events') }}
)

select * from source

