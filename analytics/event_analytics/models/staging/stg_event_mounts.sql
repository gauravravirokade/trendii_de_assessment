{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT
        event_created_at,
        event_id,
        publisher_id,
        domain,
        url,
        device_id,
        user_agent,
        page_view_id,
        event_data_json,
        source_file_name
    FROM {{ ref('src_events') }}
    WHERE event_name = 'Mounts'
),

flattened AS (
    SELECT
        b.*,
        JSONB_ARRAY_ELEMENTS(b.event_data_json -> 'mounts') AS mount_json
    FROM base b
),

final AS (
    SELECT
        f.event_created_at,
        f.event_id,
        f.publisher_id,
        f.domain,
        f.url,
        f.device_id,
        f.user_agent,
        f.page_view_id,
        f.event_data_json,
        f.source_file_name,
        TRIM(COALESCE(mount_json ->> 'image_id', ''))    AS image_id,
        CAST(mount_json ->> 'mount_index' AS INT)        AS mount_index
    FROM flattened f
)

SELECT *
FROM final
