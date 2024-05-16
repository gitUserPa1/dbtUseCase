{{
  config(
    materialized = 'view'
    )
}}

WITH emp_profile AS(
    SELECT * FROM {{ ref('src_emp_data') }}
)

SELECT 
    raw_id,
    department,
    designation
FROM
    emp_profile