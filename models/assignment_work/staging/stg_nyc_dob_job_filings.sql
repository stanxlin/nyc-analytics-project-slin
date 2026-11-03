WITH source AS (
   SELECT * FROM {{ source('raw', 'source_dobnow_job_filings_homework') }}
),

cleaned AS (
   SELECT
       * EXCEPT (
           job_filing_number,
           filing_status,
           job_type,
           filing_date,
           current_status_date,
           first_permit_date,
           approved_date,
           signoff_date,
           borough,
           house_no,
           street_name,
           zip,
           postcode,
           latitude,
           longitude,
           initial_cost,
           commmunity_board,
           owner_s_business_name,
           existing_stories,
           proposed_no_of_stories,
           existing_height,
           proposed_height,
           total_construction_floor_area,
           existing_dwelling_units,
           proposed_dwelling_units
       ),

       CAST(job_filing_number AS STRING) AS job_filing_id,

       CAST(filing_date AS TIMESTAMP) AS created_date,
       CAST(current_status_date AS TIMESTAMP) AS current_status_date,
       CAST(first_permit_date AS TIMESTAMP) AS first_permit_date,
       CAST(approved_date AS TIMESTAMP) AS approved_date,
       CAST(signoff_date AS TIMESTAMP) AS signoff_date,

       UPPER(TRIM(CAST(filing_status AS STRING))) AS status,
       CAST(job_type AS STRING) AS job_type,

       SAFE_CAST(REGEXP_REPLACE(CAST(initial_cost AS STRING), r'[$,]', '') AS NUMERIC) AS initial_cost,
       SAFE_CAST(REGEXP_REPLACE(CAST(existing_stories AS STRING), r'[,]', '') AS FLOAT64) AS existing_stories,
       SAFE_CAST(REGEXP_REPLACE(CAST(proposed_no_of_stories AS STRING), r'[,]', '') AS FLOAT64) AS proposed_stories,
       SAFE_CAST(REGEXP_REPLACE(CAST(existing_height AS STRING), r'[,]', '') AS FLOAT64) AS existing_height,
       SAFE_CAST(REGEXP_REPLACE(CAST(proposed_height AS STRING), r'[,]', '') AS FLOAT64) AS proposed_height,
       SAFE_CAST(REGEXP_REPLACE(CAST(total_construction_floor_area AS STRING), r'[,]', '') AS FLOAT64) AS total_construction_floor_area,
       SAFE_CAST(REGEXP_REPLACE(CAST(existing_dwelling_units AS STRING), r'[,]', '') AS FLOAT64) AS existing_dwelling_units,
       SAFE_CAST(REGEXP_REPLACE(CAST(proposed_dwelling_units AS STRING), r'[,]', '') AS FLOAT64) AS proposed_dwelling_units,

       CAST(commmunity_board AS STRING) AS community_board,
       CAST(owner_s_business_name AS STRING) AS owner_business_name,

       CASE
           WHEN UPPER(TRIM(CAST(COALESCE(zip, postcode) AS STRING))) IN ('N/A', 'NA') THEN NULL
           WHEN LENGTH(TRIM(CAST(COALESCE(zip, postcode) AS STRING))) = 5 THEN TRIM(CAST(COALESCE(zip, postcode) AS STRING))
           WHEN LENGTH(TRIM(CAST(COALESCE(zip, postcode) AS STRING))) = 9 THEN TRIM(CAST(COALESCE(zip, postcode) AS STRING))
           WHEN LENGTH(TRIM(CAST(COALESCE(zip, postcode) AS STRING))) = 10
               AND REGEXP_CONTAINS(TRIM(CAST(COALESCE(zip, postcode) AS STRING)), r'^\d{5}-\d{4}')
           THEN TRIM(CAST(COALESCE(zip, postcode) AS STRING))
           ELSE NULL
       END AS zip,

       CASE
           WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
           WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
           WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
           WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
           WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
           ELSE 'UNKNOWN or CITYWIDE'
       END AS borough,

       TRIM(CONCAT(COALESCE(CAST(house_no AS STRING), ''), ' ', COALESCE(CAST(street_name AS STRING), ''))) AS incident_address,
       CAST(street_name AS STRING) AS street_name,
       CAST(latitude AS DECIMAL) AS latitude,
       CAST(longitude AS DECIMAL) AS longitude,

       CURRENT_TIMESTAMP() AS _stg_loaded_at

   FROM source

   WHERE job_filing_number IS NOT NULL
   AND filing_date IS NOT NULL
   AND CAST(filing_date AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 YEAR)
   AND borough IS NOT NULL

   QUALIFY ROW_NUMBER() OVER (PARTITION BY job_filing_number ORDER BY filing_date DESC) = 1
)

SELECT * FROM cleaned