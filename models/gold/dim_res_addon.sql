{{
    config(
        schema = 'SEAWARE',
        database = 'AMA_DEV_GLD',
        materialized='incremental',
        unique_key='DIM_RES_ADDON_ID',
        incremental_strategy = 'merge' 
    )
}}


SELECT
   {{dbt_utils.generate_surrogate_key(["DATA_SOURCE",'RES_ADDON_ID','START_DATE'])}} AS DIM_RES_ADDON_ID,
    DATA_SOURCE,
    RES_ADDON_ID,
    GUEST_ID,
    RES_ID,
    RES_ADDON_CODE,
    START_DATE,
    END_DATE,
    EFFECTIVE_DATE,
    IS_DEFAULT,
    PAP_CODE,
    QUANTITY,
    IS_AUTO,
    HOTEL_ROOM_REQUEST_ID,
    MANDATORY_GROUP,
    RES_PACKAGE_ID,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("res_addon") }}