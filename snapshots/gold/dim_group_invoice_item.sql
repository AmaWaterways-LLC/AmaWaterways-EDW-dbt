{% snapshot dim_group_invoice_item%}

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
            "dbt_scd_id": "dim_group_invoice_item_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    RECORD_ID,
    GROUP_ID,
    REC_SEQN,
    INVOICE_ITEM_TYPE,
    INVOICE_ITEM_SUBTYPE,
    EFF_DATE,
    INVOICE_ITEM_SUBTYPE2,
    PROMO_CODE,
    GROUP_SHIP_REQ_ID,
    GROUP_HOTEL_REQ_ID,
    GROUP_PACKAGE_ID,
    PACKAGE_ID,
    COMMISSION_PERCENT,
    GUEST_SEQN,
    PRICE_AREA,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED,
    QUANTITY,
    BROCHURE_PRICE,
    GROUP_PRICE,
    TOTAL_PRICE,
FROM {{ ref("group_invoice_item") }}


{% endsnapshot %}
