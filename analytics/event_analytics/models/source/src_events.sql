/*
 Purpose - raw event data from Postgres; parses event_context to get event_id, publisher_id, domain, url, device_id, user_agent, and page_view_id.
Grain: one row per event.
 */

WITH
    base AS (SELECT
                 *
                 -- Cast JSON columns to JSONB
               , event_context::jsonb AS context
               , event_data::jsonb    AS event_data_json
             FROM
                 {{ source('raw', 'raw_events') }}
-- raw.raw_events
    )

  , final AS (SELECT
                  event_created_at
                , event_name
                  -- Parsed context fields
                , context ->> 'eid'          AS event_id
                , context ->> 'publisher_id' AS publisher_id
                , context ->> 'domain'       AS domain
                , context ->> 'url'          AS url
                , context ->> 'did'          AS device_id
                , context ->> 'ua'           AS user_agent
                , context ->> 'pvid'         AS page_view_id
                , event_data_json
                , context
                , source_file_name
              FROM
                  base)

SELECT
    *
FROM
    final

