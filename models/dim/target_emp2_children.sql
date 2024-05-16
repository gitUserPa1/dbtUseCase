{{
  config(
    materialized = 'incremental',
    on_schema_change='fail',
    unique_key = 'childid'
    )
}}


WITH employee_children AS(
    SELECT * FROM {{ ref('stage_src_emp2_table') }}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['empid', 'child_name', 'child_gender']) }} AS childid,
  empid,
  child_name as name,
  {{ process_gender('child_gender') }} AS gender,
  child_age as age,
  {{ audit_column() }}
  FROM employee_children
GROUP BY raw_id, empid, child_name, child_gender, child_age, created_at, updated_at

{% if is_incremental() %}
  having created_at > (select max(created_at) from {{ this }})
{% endif %}