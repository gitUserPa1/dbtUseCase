{{
  config(
    materialized = 'incremental',
    on_schema_change='fail',
    unique_key = 'yearid'
    )
}}

WITH employee_cities_years AS(
    SELECT * FROM {{ ref('stage_src_emp2_table') }}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['empid', 'year']) }} AS yearid,
  empid,
  year,
  {{ audit_column() }}
FROM employee_cities_years
GROUP BY raw_id, empid, year, created_at, updated_at

{% if is_incremental() %}
  having created_at > (select max(created_at) from {{ this }})
{% endif %}


