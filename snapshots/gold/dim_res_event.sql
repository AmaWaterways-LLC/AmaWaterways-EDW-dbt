{% snapshot dim_res_event%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['RECORD_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_res_event_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    RECORD_ID,
    RES_EVENT_TYPE,
    RES_ID,
    EVENT_TIMESTAMP,
    OPERATOR_ID,
    OLD_STATUS,
    NEW_STATUS,
    OLD_SHIP_CODE,
    OLD_SAIL_FROM,
    OLD_SAIL_TO,
    NEW_SHIP_CODE,
    NEW_SAIL_FROM,
    NEW_SAIL_TO,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("res_event") }}


{% endsnapshot %}
