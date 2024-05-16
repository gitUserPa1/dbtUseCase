{% macro process_gender(gender_column) %}
    CASE
        WHEN UPPER({{gender_column}}) = 'MALE' THEN 'M'
        WHEN UPPER({{gender_column}}) = 'FEMALE' THEN 'F'
        ELSE 'Unknown'
    END
{% endmacro %}