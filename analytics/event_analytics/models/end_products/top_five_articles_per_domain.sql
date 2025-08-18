
{{ config(
    MATERIALIZED='table'
) }}

WITH
    domain_traffic
        AS (SELECT
                     domain
                   , url
                   , COUNT(*) AS cnt_tagloads
                 FROM
--                      grr_dev.stg_event_tag_loaded
{{ ref('stg_event_tag_loaded') }}
                 GROUP BY
                     domain, url)

  , ranked_traffic
      AS (SELECT
                           domain
                         , url
                         , cnt_tagloads
                         , ROW_NUMBER() OVER (PARTITION BY domain ORDER BY cnt_tagloads DESC) AS traffic_rank
                       FROM
                           domain_traffic)
SELECT
    domain
  , url
  , cnt_tagloads
FROM
    ranked_traffic
WHERE
    traffic_rank <= 5
ORDER BY
    domain, traffic_rank