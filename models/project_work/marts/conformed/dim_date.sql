WITH stg_311 AS (
    SELECT * FROM {{ ref('stg_traffic_signal_requests') }}
),

all_dates AS (

    SELECT DISTINCT CAST(created_date AS DATE) AS full_date
    FROM stg_311
    WHERE created_date IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT CAST(closed_date AS DATE) AS full_date
    FROM stg_311
    WHERE closed_date IS NOT NULL

),

date_dimension AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['full_date']) }} AS date_key,
        full_date,
        EXTRACT(YEAR FROM full_date) AS year,
        EXTRACT(QUARTER FROM full_date) AS quarter,
        EXTRACT(MONTH FROM full_date) AS month,
        EXTRACT(DAY FROM full_date) AS day,
        FORMAT_DATE('%A', full_date) AS day_of_week,
        EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) AS is_weekend,
        CASE
            WHEN EXTRACT(MONTH FROM full_date) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM full_date) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM full_date) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END AS season
    FROM all_dates

),

null_placeholder AS (

    SELECT
        '-1' AS date_key,
        CAST(NULL AS DATE) AS full_date,
        CAST(NULL AS INT64) AS year,
        CAST(NULL AS INT64) AS quarter,
        CAST(NULL AS INT64) AS month,
        CAST(NULL AS INT64) AS day,
        CAST(NULL AS STRING) AS day_of_week,
        CAST(NULL AS BOOLEAN) AS is_weekend,
        CAST(NULL AS STRING) AS season

)

SELECT * FROM date_dimension

UNION ALL

SELECT * FROM null_placeholder