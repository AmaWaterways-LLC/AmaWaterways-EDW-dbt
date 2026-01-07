{% snapshot dim_sp_req_order%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['SP_REQ_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_sp_req_order_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    SP_REQ_ID,
    ORDER_DATE,
    ORD_STATUS_CODE,
    ORDER_VALID,
    AM_BOOKING_TYPE,
    RES_ID,
    AGENCY_ID,
    TERMINATION_COMPLETED,
    SOURCE_DELETED,
    LAST_UPDATED_TIMESTAMP
FROM {{ ref("sp_req_order") }}


{% endsnapshot %}
