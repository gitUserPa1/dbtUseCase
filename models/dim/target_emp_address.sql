{{
  config(
    materialized = 'view'
    )
}}

WITH emp_address AS(
    SELECT * FROM {{ ref('src_emp_data') }}
)

SELECT 
    raw_id,
    streetaddress,
    city,
    postalcode
FROM
    emp_address