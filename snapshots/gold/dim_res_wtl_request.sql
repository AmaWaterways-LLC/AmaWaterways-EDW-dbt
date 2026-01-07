{% snapshot dim_res_wtl_request%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['REQUEST_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_res_wtl_request_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    REQUEST_ID,
    RES_ID,
    PRIORITY,
    REQUEST_TYPE,
    SER_STR_ID,
    COMMENTS,
    IS_AUTO,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("res_wtl_request") }}


{% endsnapshot %}
