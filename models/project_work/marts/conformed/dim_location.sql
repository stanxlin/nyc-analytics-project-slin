WITH stg_311 AS (
    SELECT * FROM {{ ref('stg_traffic_signal_requests') }}
),

all_locations AS (

    SELECT DISTINCT
        borough,
        zip AS zip_code
    FROM stg_311
    WHERE borough IS NOT NULL
       OR zip IS NOT NULL

),

location_dimension AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['borough', 'zip_code']) }} AS location_key,
        zip_code,
        borough
    FROM all_locations

),

null_placeholder AS (

    SELECT
        '-1' AS location_key,
        CAST(NULL AS STRING) AS zip_code,
        CAST(NULL AS STRING) AS borough

)

SELECT * FROM location_dimension

UNION ALL

SELECT * FROM null_placeholder