WITH raw_emp_personal AS (
    {# SELECT * FROM AIRBNB.RAW.raw_data2 #}

    SELECT * FROM {{ source('airbnb', 'empjsonexpanded') }}
)

select
  
  raw.value:empid::string as empid,
  raw.value:kind::string as kind,
  raw.value:fullName::string as fullName,
  raw.value:age::number as age,
  raw.value:gender::string as gender,
  raw.value:phoneNumber:areaCode::string as areaCode,
  raw.value:phoneNumber:number::string as number,
  CURRENT_TIMESTAMP AS timestamp_column
  
from raw_emp_personal,
lateral flatten(input => data_content:root, outer => true) as raw