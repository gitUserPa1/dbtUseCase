WITH raw_emp_children AS (
    {# SELECT * FROM AIRBNB.RAW.raw_data2 #}

    SELECT * FROM {{ source('airbnb', 'empjsonexpanded') }}
)

select

  raw_emp.value:empid::string as empid,
  raw.value:name::string as name,
  raw.value:gender::string as gender,
  raw.value:age::string as age,
  CURRENT_TIMESTAMP AS timestamp_column

from raw_emp_children as raw_child,

lateral flatten(input => raw_child.data_content:root, outer => true) as raw_emp,
lateral flatten(input => raw_emp.value:children, outer => true) as raw
