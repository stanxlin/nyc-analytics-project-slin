WITH minutes AS (
    SELECT minute_of_day
    FROM UNNEST(GENERATE_ARRAY(0, 1439)) AS minute_of_day
),

time_dimension AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['minute_of_day']) }} AS time_key,
        DIV(minute_of_day, 60) AS hour,
        MOD(minute_of_day, 60) AS minute,
        CASE
            WHEN DIV(minute_of_day, 60) BETWEEN 5 AND 11 THEN 'Morning'
            WHEN DIV(minute_of_day, 60) BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN DIV(minute_of_day, 60) BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day
    FROM minutes

),

null_placeholder AS (

    SELECT
        '-1' AS time_key,
        CAST(NULL AS INT64) AS hour,
        CAST(NULL AS INT64) AS minute,
        CAST(NULL AS STRING) AS time_of_day

)

SELECT * FROM time_dimension

UNION ALL

SELECT * FROM null_placeholder