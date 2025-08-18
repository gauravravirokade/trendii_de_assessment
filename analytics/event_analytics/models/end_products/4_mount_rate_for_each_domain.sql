WITH
    tagload_counts AS (
        -- Step 1: Count total tagloads for each domain
        SELECT
            domain,
            COUNT( DISTINCT event_id) AS total_tagloads
        FROM {{ ref('stg_event_tag_loaded') }}
--             trendii_de_assessment.grr_dev.stg_event_tag_loaded
        GROUP BY
            domain
    ),

    mount_counts AS (
        -- Step 2: Count total mounts for each domain
        SELECT
            domain,
            COUNT(DISTINCT event_id) AS total_mounts
        FROM {{ ref('stg_event_mounts') }}
--             trendii_de_assessment.grr_dev.stg_event_mounts
        GROUP BY
            domain
    )

-- Step 3: Join and calculate the mount rate, rounded to 2 decimal places
SELECT
    t.domain,
    t.total_tagloads,
    COALESCE(m.total_mounts, 0) AS total_mounts,
    ROUND((COALESCE(m.total_mounts, 0)::DECIMAL / t.total_tagloads) * 100, 2) AS mount_rate_percent
FROM
    tagload_counts AS t
LEFT JOIN
    mount_counts AS m
        ON t.domain = m.domain


