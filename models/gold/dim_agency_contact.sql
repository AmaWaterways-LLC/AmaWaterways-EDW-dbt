{{
    config(
        schema = 'SEAWARE',
        database = 'AMA_DEV_GLD',
        materialized='incremental',
        unique_key='DIM_AGENCY_CONTACT_ID',
        incremental_strategy = 'merge' 
    )
}}


SELECT 
    {{dbt_utils.generate_surrogate_key(["DATA_SOURCE",'AGENCY_CONTACT_ID','DATE_FROM'])}} AS DIM_AGENCY_CONTACT_ID,
    DATA_SOURCE,
    AGENCY_CONTACT_ID,
    AGENCY_ID,
    AGENT_ID,
    DATE_FROM,
    DATE_TO,
    POSITION,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED,
FROM {{ref("agency_contact")}}