{{
    config(
        schema = 'SEAWARE',
        database = 'AMA_DEV_GLD',
        materialized='incremental',
        unique_key='DIM_AGENCY_CLASSIFICATION_LINK_ID',
        incremental_strategy = 'merge' 
    )
}}


SELECT 
    {{dbt_utils.generate_surrogate_key(["DATA_SOURCE",'RECORD_ID','DATE_FROM'])}} AS DIM_AGENCY_CLASSIFICATION_LINK_ID,
    DATA_SOURCE,
    DATE_FROM,
    DATE_TO,
    AGENCY_CLASSIFICATION_CODE,
    AGENCY_ID,
    RECORD_ID,
    AGENCY_CLASS_TYPE,
    LAST_UPDATED_TIMESTAMP AS updated_at,
    SOURCE_DELETED,
FROM {{ref("agency_classification_link")}}