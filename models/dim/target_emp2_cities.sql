{{
  config(
    materialized = 'incremental',
    on_schema_change='fail',
    unique_key = 'cityid'
    )
}}

WITH employee_cities AS(
    SELECT * FROM {{ ref('stage_src_emp2_table') }}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['empid', 'city_name','created_at']) }} AS cityid,
  empid,
  city_name as city,
  {{ audit_column() }}
  FROM employee_cities
GROUP BY raw_id, empid, city_name, created_at, updated_at

{% if is_incremental() %}
  having created_at > (select max(created_at) from {{ this }})
{% endif %}

