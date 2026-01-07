{{
    config(
        schema = 'SEAWARE',
        database = 'AMA_DEV_GLD',
        materialized='incremental',
        unique_key='DIM_RES_HOTEL_REQUEST_ID',
        incremental_strategy = 'merge' 
    )
}}


SELECT
   {{dbt_utils.generate_surrogate_key(["DATA_SOURCE",'HOTEL_REQUEST_ID','DATE_FROM'])}} AS DIM_RES_HOTEL_REQUEST_ID,
    DATA_SOURCE,
    HOTEL_REQUEST_ID,
    RES_ID,
    DATE_FROM,
    DATE_TO,
    HOTEL_REQUEST_TYPE,
    HOTEL_CATEGORY,
    HOTEL_ROOM_CATEGORY,
    HOTEL_ID,
    HOTEL_ROOM_TYPE,
    ALLOCATION_ID,
    INVENTORY_REQUEST_TYPE,
    INVENTORY_RESULT_TYPE,
    HOTEL_CATEGORY_PACKAGE,
    OCCUPANCY,
    CITY_CODE,
    PACKAGE_ID,
    HOTEL_SPACE_TYPE,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("res_hotel_request") }}