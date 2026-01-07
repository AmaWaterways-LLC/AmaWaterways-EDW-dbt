{% snapshot dim_group_booking%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['GROUP_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_group_booking_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    GROUP_ID,
    COMMENTS,
    AGENCY_ID,
    AGENT_ID,
    GROUP_NAME,
    GROUP_TYPE,
    N_OF_GUESTS,
    REL_DAY_FROM,
    REL_DAY_TO,
    SAIL_DATE_FROM,
    SAIL_DATE_TO,
    SHIP_CODE,
    GROUP_STATUS,
    GROUP_INIT_DATE,
    CURRENCY_CODE,
    SEC_AGENCY_ID
    OFFICE_CODE,
    FIRST_FULL_PAYMENT_PROCESSED,
    DEP_REF_ID,
    ARR_REF_ID,
    IS_PAYSCH_MANUAL,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED,
FROM {{ ref("group_booking") }}


{% endsnapshot %}
