{% snapshot dim_tw_res_detail%}

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
            "dbt_scd_id": "dim_tw_res_details_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
DATA_SOURCE,
RES_ID,
TW_RES_HEADER_ID,
TW_STATUS,
SOURCE_DELETED,
LAST_UPDATED_TIMESTAMP
FROM {{ ref("tw_res_detail") }}


{% endsnapshot %}
