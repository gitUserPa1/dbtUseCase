{{
    config(
        materialized = 'incremental',
        unique_key='empid',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
) }}

WITH tgt_emp2_personal AS (
    SELECT * FROM {{ ref('target_emp2_personal') }}
)
SELECT 
  *
FROM tgt_emp2_personal

{% if is_incremental() %}
  where created_at > (select max(created_at) from {{ this }})
{% endif %}