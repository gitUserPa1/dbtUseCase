WITH stage_emp_raw AS (
    SELECT * FROM {{ ref('src_emp2_table') }}
)
SELECT 
    *
FROM stage_emp_raw