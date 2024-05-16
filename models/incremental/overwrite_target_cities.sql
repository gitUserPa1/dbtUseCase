{{
    config(
        materialized = 'incremental',
        unique_key='cityid',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
) }}

WITH tgt_emp2_cities AS (
    SELECT * FROM {{ ref('target_emp2_cities') }}
)
SELECT 
  *
  FROM tgt_emp2_cities

{% if is_incremental() %}
  where created_at > (select max(created_at) from {{ this }})
{% endif %}
