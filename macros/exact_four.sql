{% test exact_four(model, column_name) %}

SELECT * FROM {{ model }} WHERE LENGTH({{ column_name}}) != 4 AND {{ column_name}} NOT LIKE '%[^0-9]%' 

{% endtest %}