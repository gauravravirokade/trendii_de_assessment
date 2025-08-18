{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

WITH
    base AS (SELECT
                 *
             FROM
                 {{ ref('src_events') }}
             WHERE
                 event_name = 'TagLoaded')

  , final AS (SELECT
                  event_created_at
                , event_id
                , publisher_id
                , domain
                , url
                , device_id
                , user_agent
                , page_view_id
                , event_data_json
              FROM
                  base)

SELECT
    *
FROM
    final
