{% test exact_ten(model, column_name) %}

SELECT * FROM {{ model }} WHERE LENGTH({{ column_name}}) != 10 AND {{ column_name}} NOT LIKE '%[^0-9]%' 

{% endtest %}