
{{
    config(
        schema = 'SEAWARE',
        database = 'AMA_DEV_GLD',
        materialized='incremental',
        unique_key='DIM_RES_GUEST_IN_ROOM_ID',
        incremental_strategy = 'merge' 
    )
}}

SELECT
   {{dbt_utils.generate_surrogate_key(["DATA_SOURCE",'RECORD_ID','DATE_FROM'])}} AS DIM_RES_GUEST_IN_ROOM_ID,
    DATA_SOURCE,
    RECORD_ID,
    RES_ID,
    ROOM_SEQ_NUMBER,
    GUEST_ID,
    DATE_FROM,
    DATE_TO,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("res_guest_in_room") }}