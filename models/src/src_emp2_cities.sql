WITH raw_emp_cities AS (
    {# SELECT * FROM AIRBNB.RAW.raw_data2 #}

    SELECT * FROM {{ source('airbnb', 'empjsonexpanded') }}
)

select
  
  raw_emp.value:empid::string as empid,
  raw.value:place::string as place,

  raw_years.value::string as yearsLived,
  CURRENT_TIMESTAMP AS timestamp_column


from raw_emp_cities as raw_cities,

lateral flatten(input => raw_cities.data_content:root, outer => true) as raw_emp,
lateral flatten(input => raw_emp.value:citiesLived, outer => true) as raw,
lateral flatten(input => raw.value:yearsLived, outer => true) as raw_years


