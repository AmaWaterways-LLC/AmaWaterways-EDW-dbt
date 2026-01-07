{% snapshot dim_cabin_category%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['CABIN_CATEGORY_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_cabin_category_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    CABIN_CATEGORY,
    CABIN_CATEGORY_ID,
    COMMENTS,
    IMAGE_ID,
    CABIN_CAPACITY,
    CABIN_CATEGORY_RANK,
    SHIP_CODE,
    IS_WITHOUT_CABINS,
    CATEGORY_CAPACITY,
    IS_ACTIVE,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("cabin_category") }}


{% endsnapshot %}
