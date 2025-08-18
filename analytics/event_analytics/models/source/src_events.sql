{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

WITH base AS (
    SELECT
        event_created_at,
        event_name,
        event_context::jsonb AS context,
        event_data::jsonb AS event_data_json,
        source_file_name
    FROM {{ source('raw', 'raw_events') }}
),

final AS (
    SELECT
        b.event_created_at,
        b.event_name,
TRIM(b.context ->> 'eid')          AS event_id,
TRIM(b.context ->> 'publisher_id') AS publisher_id,
TRIM(b.context ->> 'domain')       AS domain,
TRIM(b.context ->> 'url')          AS url,
TRIM(b.context ->> 'did')          AS device_id,
TRIM(b.context ->> 'ua')           AS user_agent,
TRIM(b.context ->> 'pvid')         AS page_view_id,

        b.event_data_json,
        b.context,
        b.source_file_name
    FROM base b
    {% if is_incremental() %}
    LEFT JOIN {{ this }} t
        ON b.context ->> 'eid' = t.event_id
    WHERE t.event_id IS NULL
    {% endif %}
)

SELECT *
FROM final
