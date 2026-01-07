{{
    config(
        schema = 'SEAWARE',
        database = 'AMA_DEV_GLD',
        materialized='incremental',
        unique_key='DIM_AGENCY_REGION_LINK_ID',
        incremental_strategy = 'merge' 
    )
}}


SELECT 
    {{dbt_utils.generate_surrogate_key(["DATA_SOURCE",'AG_REGION_LINK_ID','DATE_FROM'])}} AS DIM_AGENCY_REGION_LINK_ID,
    DATA_SOURCE,
    AG_REGION_LINK_ID,
    AGENCY_ID,
    DATE_FROM,
    DATE_TO,
    COMMENTS,
    REGION_ID,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED,
FROM {{ref("agency_region_link")}}