{{
    config(
        schema = 'SEAWARE',
        database = 'AMA_DEV_GLD',
        materialized='incremental',
        unique_key='DIM_GROUP_LAND_PACKAGE_ID',
        incremental_strategy = 'merge' 
    )
}}


SELECT
    {{dbt_utils.generate_surrogate_key(["DATA_SOURCE",'GROUP_PACKAGE_ID','START_DATE'])}} AS DIM_GROUP_LAND_PACKAGE_ID,
    DATA_SOURCE,
    GROUP_PACKAGE_ID,
    GROUP_ID,
    PACKAGE_ID,
    PACKAGE_TYPE,
    START_DATE,
    END_DATE,
    EFFECTIVE_DATE,
    SHIP_CODE,
    CABIN_CATEGORY,
    OCCUPANCY,
    QUANTITY,
    IS_PRICEABLE,
    LOCATION_TYPE_FROM,
    LOCATION_CODE_FROM,
    LOCATION_TYPE_TO,
    LOCATION_CODE_TO,
    PACKAGE_STATUS,
    TOUR_PACKAGE_ID,
    TOUR_PACKAGE_ITIN_ID,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED,
FROM {{ ref("group_land_package") }}