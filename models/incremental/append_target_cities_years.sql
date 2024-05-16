{{
    config(
        materialized = 'incremental',
        unique_key='yearid',
        incremental_strategy='append',
        on_schema_change='fail'
) }}

WITH tgt_emp2_years AS (
    SELECT * FROM {{ ref('target_emp2_cities_years') }}
)
SELECT 
  *
FROM tgt_emp2_years

{% if is_incremental() %}
  where created_at > (select max(created_at) from {{ this }})
{% endif %}