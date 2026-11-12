WITH stg_311 AS (
    SELECT * FROM {{ ref('stg_traffic_signal_requests') }}
),

distinct_problems AS (

    SELECT DISTINCT
        complaint_type,
        descriptor AS problem_component,
        descriptor_2 AS problem_condition
    FROM stg_311
    WHERE complaint_type IS NOT NULL

),

enriched AS (

    SELECT
        complaint_type,
        problem_component,
        problem_condition,

        CASE
            WHEN LOWER(IFNULL(problem_condition, '')) LIKE '%install%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%remove%'
            THEN 'installation or removal'

            WHEN LOWER(IFNULL(problem_condition, '')) LIKE '%need%'
            THEN 'maintenance request'

            WHEN LOWER(IFNULL(problem_condition, '')) LIKE '%out%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%down%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%dark%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%not working%'
            THEN 'outage'

            WHEN LOWER(IFNULL(problem_condition, '')) LIKE '%improper%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%steady%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%dim%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%flash%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%stuck%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%timing%'
            THEN 'malfunction'

            WHEN LOWER(IFNULL(problem_condition, '')) LIKE '%damag%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%loose%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%hanging%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%exposed%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%missing%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%knocked%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%leaning%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%vandal%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%crack%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%open%'
            THEN 'physical damage'

            WHEN LOWER(IFNULL(problem_condition, '')) LIKE '%obscured%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%dirty%'
              OR LOWER(IFNULL(problem_condition, '')) LIKE '%turned%'
            THEN 'visibility or alignment'

            ELSE 'other'
        END AS problem_category

    FROM distinct_problems

),

dim_problem AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'complaint_type', 'problem_component', 'problem_condition'
        ]) }} AS problem_key,

        complaint_type,
        problem_component,
        problem_condition,
        problem_category

    FROM enriched

)

SELECT * FROM dim_problem