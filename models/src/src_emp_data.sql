WITH raw_data AS (
    {# SELECT * FROM AIRBNB.RAW.RAW_DATA #}

    SELECT * FROM {{ source('airbnb', 'empjsondata') }}
)

select
  raw_id,
  raw.data_content:empid::string as empid,
  raw.data_content:personal:name::string as name,
  raw.data_content:personal:gender::string as gender,
  raw.data_content:personal:age::number as age,
  raw.data_content:personal:address:streetaddress::string as streetaddress,
  raw.data_content:personal:address:city::string as city,
  raw.data_content:personal:address:state::string as state,
  raw.data_content:personal:address:postalcode::string as postalcode,
  raw.data_content:profile:department::string as department,
  raw.data_content:profile:designation::string as designation
from raw_data as raw