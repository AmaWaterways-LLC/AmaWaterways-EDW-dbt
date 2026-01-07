{% snapshot dim_agent_address%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['AGENT_ADDR_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_agent_address_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    AGENT_ID,
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
    COMMENTS,
    AGENT_ADDR_ID,
    ADDRESS_LINE3,
    ADDRESS_LINE4,
    DATE_FROM,
    DATE_TO,
    ARE_DATES_EVERY_YEAR,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("agent_address") }}


{% endsnapshot %}
