{{
  config(
    materialized = 'incremental',
    on_schema_change='fail',
    unique_key = 'empid'
    )
}}

WITH employee_profile AS(
    SELECT * FROM {{ ref('stage_src_emp2_table') }}
)

SELECT 
  empid,
  kind,
  fullName,
  age,
  {{ process_gender('gender') }} AS gender,
  areaCode,
  phoneNumber,
  {{ audit_column() }}
FROM employee_profile
GROUP BY raw_id, empid, kind, fullName, age, gender, areaCode, phoneNumber, created_at, updated_at

{% if is_incremental() %}
  having created_at > (select max(created_at) from {{ this }})
{% endif %}


