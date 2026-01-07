{% snapshot dim_group_ship_request%}

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
            "dbt_scd_id": "dim_group_ship_request_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    RECORD_ID,
    CABIN_CATEGORY,
    OCCUPANCY,
    QUANTITY,
    PRODUCT_TYPE,
    PACKAGE_TYPE,
    SHIP_CODE,
    GROUP_ID,
    EFFECTIVE_DATE,
    PACKAGE_ID,
    DEP_REF_ID,
    ARR_REF_ID,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED,
FROM {{ ref("group_ship_request") }}


{% endsnapshot %}
