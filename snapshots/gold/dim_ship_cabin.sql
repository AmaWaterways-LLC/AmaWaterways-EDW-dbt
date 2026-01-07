{% snapshot dim_ship_cabin%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['CABIN_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_ship_cabin_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    SHIP_CODE,
    CABIN_NUMBER,
    CABIN_ID,
    DECK_NUMBER,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("ship_cabin") }}


{% endsnapshot %}
