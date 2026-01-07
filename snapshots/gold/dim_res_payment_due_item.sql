{% snapshot dim_res_payment_due_item%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['RES_PAYMENT_DUE_ITEM_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_res_payment_due_item_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    RES_PAYMENT_DUE_ITEM_ID,
    PMNT_DUE_ITEM_TYPE,
    GUEST_ID,
    RES_ID,
    DUE_DATE,
    AMOUNT,
    EXPIRATION_DATE,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("res_payment_due_item") }}


{% endsnapshot %}
