WITH raw_emp_data AS (
    SELECT * FROM AIRBNB.RAW.raw_data3

    {# SELECT * FROM {{ source('airbnb', 'empjsonexpanded') }} #}
)

SELECT 
    raw_id,
    data.value:empid::string as empid,
    data.value:kind::string as kind,
    data.value:fullName::string as fullName,
    data.value:age::number as age,
    data.value:gender::string as gender,
    data.value:phoneNumber:areaCode::number as areaCode,
    data.value:phoneNumber:number::number as phoneNumber,
    child.value:name::string as child_name,
    child.value:gender::string as child_gender,
    child.value:age::number as child_age,
    city.value:place::string as city_name,
    year.value::number as year,
    created_at,
    updated_at
    
FROM raw_emp_data,

    LATERAL FLATTEN(data_content:root, outer => true) as data,
    LATERAL FLATTEN(
        COALESCE(
            data.value:children, array_construct(
                object_construct('name', 'Unknown', 'gender', 'Unknown', 'age', 0)
                )
        )
    ) as child,
    
    LATERAL FLATTEN(data.value:citiesLived, outer => true) as city,
    LATERAL FLATTEN(city.value:yearsLived, outer => true) as year