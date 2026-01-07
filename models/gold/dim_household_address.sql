{{
    config(
        schema = 'SEAWARE',
        database = 'AMA_DEV_GLD',
        materialized='incremental',
        unique_key='DIM_HOUSEHOLD_ADDRESS_ID',
        incremental_strategy = 'merge' 
    )
}}




SELECT
   {{dbt_utils.generate_surrogate_key(["DATA_SOURCE",'AG_REGION_LINK_ID','DATE_FROM'])}} AS DIM_HOUSEHOLD_ADDRESS_ID,
    DATA_SOURCE,
    HOUSEHOLD_ID,
    SEQ_NUMBER,
    ADDRESS_TYPE,
    ADDRESS_LINE1,
    ADDRESS_LINE2,
    ADDRESS_CITY,
    STATE_CODE,
    ZIP,
    COUNTRY_CODE,
    IS_ADDRESS_MAILING,
    IS_ADDRESS_SHIPPING,
    DATE_FROM,
    DATE_TO,
    ARE_DATES_EVERY_YEAR,
    COMMENTS,
    HOUSEHOLD_ADDR_ID,
    ADDRESS_LINE3,
    ADDRESS_LINE4,
    ADDRESS_ESSENTIAL,
    CAN_CONTACT,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("household_address") }}