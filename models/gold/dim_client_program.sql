{{
    config(
        schema = 'SEAWARE',
        database = 'AMA_DEV_GLD',
        materialized='incremental',
        unique_key='DIM_CLIENT_PROGRAM_ID',
        incremental_strategy = 'merge' 
    )
}}


SELECT 
    {{dbt_utils.generate_surrogate_key(["DATA_SOURCE",'CLIENT_PROGRAM_ID','DATE_FROM'])}} AS DIM_CLIENT_PROGRAM_ID,
    DATA_SOURCE,
    CLIENT_PROGRAM_ID,
    CLIENT_ID
    DATE_FROM,
    DATE_TO,
    COMMENTS,
    FREQUENT_PGM_CODE,
    FREQUENT_PGM_ACCOUNT,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED,
FROM {{ref("client_program")}}