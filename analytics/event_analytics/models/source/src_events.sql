SELECT
    *
FROM
    {{ source('raw', 'raw_events') }}