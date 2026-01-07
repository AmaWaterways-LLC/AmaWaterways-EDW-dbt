{% snapshot dim_res_archive%}

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
            "dbt_scd_id": "dim_res_archive_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    RES_ID,
    RES_INIT_DATE,
    RES_GUEST_COUNT,
    RES_ADULT_COUNT,
    RES_CHILD_COUNT,
    AGENCY_ID,
    CABIN_CATEGORY,
    CABIN_NUMBER,
    SHIP_CODE,
    SAIL_DATE_FROM,
    SAIL_DATE_TO,
    PACKAGE_CODE,
    AIR_REQUEST_TYPE,
    CURRENCY_CODE,
    RES_AMOUNT_TOTAL,
    RES_DISCOUNT_TOTAL,
    COMMISSION_TOTAL,
    RES_STATUS,
    INSURANCE,
    RECORD_ID,
    TRAVEL_AGENT_PHONE_NBR,
    ALT_RES_ID,
    SAIL_DAYS,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("res_archive") }}


{% endsnapshot %}
