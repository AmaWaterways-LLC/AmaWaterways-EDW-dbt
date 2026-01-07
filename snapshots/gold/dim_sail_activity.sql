{% snapshot dim_sail_acitivity%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['SAIL_ACTIVITY_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_sail_activity_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    SAIL_ACTIVITY_ID,
    SAIL_EVENT_DATE_TIME,
    SAIL_EVENT_TYPE,
    PORT_CODE,
    PIER_CODE,
    MAY_EMBARK,
    MAY_DISEMBARK,
    SHIP_CODE,
    RELATIVE_DAY,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("sail_activity") }}


{% endsnapshot %}
