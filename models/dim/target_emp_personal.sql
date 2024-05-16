{{
  config(
    materialized = 'view'
    )
}}

WITH emp_personal AS(
    SELECT * FROM {{ ref('src_emp_data') }}
)

SELECT 
    raw_id,
    empid,
    name,
    gender,
    age
FROM
    emp_personal