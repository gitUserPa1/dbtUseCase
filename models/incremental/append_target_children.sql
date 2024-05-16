{{
    config(
        materialized = 'incremental',
        unique_key='childid',
        incremental_strategy='append',
        on_schema_change='fail'
) }}

WITH tgt_emp2_children AS (
    SELECT * FROM {{ ref('target_emp2_children') }}
)
SELECT 
  *
  FROM tgt_emp2_children

{% if is_incremental() %}
  where created_at > (select max(created_at) from {{ this }})
{% endif %}