{{
    config(
        schema = 'SEAWARE',
        database = 'AMA_DEV_GLD',
        materialized='incremental',
        unique_key='DIM_AGENCY_COMMISSION_ID',
        incremental_strategy = 'merge' 
    )
}}


SELECT 
    {{dbt_utils.generate_surrogate_key(["DATA_SOURCE",'RECORD_ID','DATE_FROM'])}} AS DIM_AGENCY_COMMISSION_ID,
    DATA_SOURCE,
    RECORD_ID,
    AGENCY_ID,
    COMMISS_CODE,
    DATE_FROM,
    DATE_TO,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED,
FROM {{ref("agency_commission")}}