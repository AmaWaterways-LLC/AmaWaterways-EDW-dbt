{% snapshot dim_res_hist_trans%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['TRANS_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_res_hist_trans_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    TRANS_ID,
    RES_ID,
    TRANS_DATE,
    PERFORMER,
    SUBJECT,
    COMMENTS,
    SOURCE_CODE,
    SOURCE_DELETED,
    LAST_UPDATED_TIMESTAMP
FROM {{ ref("res_hist_trans") }}


{% endsnapshot %}
