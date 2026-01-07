{% snapshot dim_am_order_header%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['AM_ORDER_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_am_order_header_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    AM_ORDER_ID,
    ORDER_DATE,
    ORDER_DESCRIPT
    ORD_STATUS_CODE,
    AM_REQSTER_TYPE,
    AGENCY_ID,
    ORDER_VALID,
    TERMINATION_COMPLETED,
    CREATED_FROM_ADDON,
    CANCEL_REFUND_DONE,
    CURRENCY_CODE,
    SOURCE_DELETED,
    LAST_UPDATED_TIMESTAMP
FROM {{ ref("am_order_header") }}


{% endsnapshot %}
