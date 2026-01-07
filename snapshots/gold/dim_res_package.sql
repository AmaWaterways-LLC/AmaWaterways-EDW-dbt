{% snapshot dim_res_package%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['RES_PACKAGE_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_res_package_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    RES_PACKAGE_ID,
    RES_ID,
    GUEST_ID,
    PACKAGE_TYPE,
    PACKAGE_ID,
    PACKAGE_CLASS,
    PACKAGE_STATUS,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("res_package") }}


{% endsnapshot %}
