{% snapshot dim_ship_cabin_ctg_link%}

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
            "dbt_scd_id": "dim_ship_cabin_ctg_link_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    RECORD_ID,
    SHIP_CODE,
    CABIN_NUMBER,
    CABIN_CATEGORY,
    EFF_SAIL_FROM,
    EFF_SAIL_TO,
    ROLLAWAY_BEDS,
    CABIN_CAPACITY,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("ship_cabin_ctg_link") }}


{% endsnapshot %}
