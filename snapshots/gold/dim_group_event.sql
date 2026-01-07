{% snapshot dim_group_event%}

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
            "dbt_scd_id": "dim_group_event_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    RECORD_ID,
    GROUP_EVENT_TYPE,
    GROUP_ID,
    EVENT_TIMESTAMP,
    EVENT_DATE,
    OPERATOR_ID,
    OLD_STATUS,
    NEW_STATUS,
    OLD_SHIP_CODE,
    OLD_SAIL_FROM,
    OLD_SAIL_TO,
    NEW_SHIP_CODE,
    NEW_SAIL_FROM,
    NEW_SAIL_TO,
    OLD_CABINS,
    OLD_GUEST_COUNT,
    NEW_CABINS,
    NEW_GUEST_COUNT,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED,
FROM {{ ref("group_event") }}


{% endsnapshot %}
