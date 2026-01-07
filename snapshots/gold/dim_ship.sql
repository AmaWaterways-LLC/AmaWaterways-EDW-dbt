{% snapshot dim_ship%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['SHIP_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_ship_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    SHIP_ID,
    SHIP_CODE,
    SHIP_NAME,
    SHIP_REGISTRY,
    SHIP_BUILT_DATE,
    SHIP_CREW_SIZE,
    SHIP_PAX_CAPACITY,
    SHIP_LENGTH,
    SHIP_BRAND,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("ship") }}


{% endsnapshot %}
