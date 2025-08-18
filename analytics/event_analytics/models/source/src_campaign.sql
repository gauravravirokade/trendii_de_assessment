
SELECT CAST(id AS VARCHAR)                   AS campaign_id,
       CAST(name AS VARCHAR)                 AS campaign_name,
       CAST(brand_id AS VARCHAR)             AS campaign_brand_id,
       CAST(product_type AS VARCHAR)         AS campaign_product_type,
       CAST(cpc_rate AS DECIMAL(10, 4))      AS campaign_cpc_rate,
       CAST(cpm_rate AS DECIMAL(10, 4))      AS campaign_cpm_rate,
       CAST(cpa_percentage AS DECIMAL(5, 4)) AS campaign_cpa_percentage,
       CAST(company_name AS VARCHAR)         AS campaign_company_name,
       CAST(company_domain AS VARCHAR)       AS campaign_company_domain,
       CAST(created_at AS TIMESTAMP)         AS campaign_created_at,
       CAST(valid_from AS TIMESTAMP)         AS campaign_valid_from,
       CAST(valid_to AS TIMESTAMP)           AS campaign_valid_to,
       CAST(current_record AS BOOLEAN)       AS campaign_current_record

FROM {{ source('raw', 'dim_campaign') }}