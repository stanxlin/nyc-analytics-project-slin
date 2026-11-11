WITH source AS (
    SELECT * FROM {{ source('raw', 'source_traffic_signal_requests') }}
),

cleaned AS (
    SELECT
        * EXCEPT (
            unique_key,
            created_date,
            closed_date,
            resolution_action_updated_date,
            agency,
            agency_name,
            complaint_type,
            descriptor,
            status,
            incident_zip,
            borough,
            council_district,
            incident_address,
            street_name,
            cross_street_1,
            cross_street_2,
            latitude,
            longitude,
            open_data_channel_type
        ),

        CAST(unique_key AS STRING) AS request_id,

        CAST(created_date AS TIMESTAMP) AS created_date,
        SAFE_CAST(closed_date AS TIMESTAMP) AS closed_date,
        SAFE_CAST(resolution_action_updated_date AS TIMESTAMP) AS resolution_action_updated_date,

        CAST(agency AS STRING) AS agency,
        CAST(agency_name AS STRING) AS agency_name,
        CAST(complaint_type AS STRING) AS complaint_type,
        CAST(descriptor AS STRING) AS descriptor,
        UPPER(TRIM(CAST(status AS STRING))) AS status,

        CASE
            WHEN UPPER(TRIM(incident_zip)) IN ('N/A', 'NA') THEN NULL
            WHEN UPPER(TRIM(incident_zip)) = 'ANONYMOUS' THEN 'Anonymous'
            WHEN LENGTH(TRIM(incident_zip)) = 5 THEN TRIM(incident_zip)
            WHEN LENGTH(TRIM(incident_zip)) = 9 THEN TRIM(incident_zip)
            WHEN LENGTH(TRIM(incident_zip)) = 10
                AND SUBSTR(TRIM(incident_zip), 6, 1) = '-'
            THEN TRIM(incident_zip)
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

        CAST(council_district AS STRING) AS council_district,
        CAST(incident_address AS STRING) AS incident_address,
        CAST(street_name AS STRING) AS street_name,
        CAST(cross_street_1 AS STRING) AS cross_street_1,
        CAST(cross_street_2 AS STRING) AS cross_street_2,
        SAFE_CAST(latitude AS NUMERIC) AS latitude,
        SAFE_CAST(longitude AS NUMERIC) AS longitude,

        CAST(open_data_channel_type AS STRING) AS method_of_submission,

        CURRENT_TIMESTAMP() AS _stg_loaded_at

    FROM source

    WHERE unique_key IS NOT NULL
      AND created_date IS NOT NULL
      AND borough IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY created_date DESC) = 1
)

SELECT * FROM cleaned