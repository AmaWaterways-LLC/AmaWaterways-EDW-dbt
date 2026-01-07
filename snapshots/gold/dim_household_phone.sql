{% snapshot dim_household_phone%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['HOUSEHOLD_PHONE_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_household_phone_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    HOUSEHOLD_ID,
    SEQ_NUMBER,
    PHONE_TYPE,
    INTL_CODE,
    PHONE_NUMBER,
    IS_PHONE_PRIMARY,
    IS_PHONE_SECONDARY,
    IS_PHONE_FAX,
    IS_PHONE_EMERGENCY,
    FAX_ATTENTION_LINE,
    COMMENTS,
    HOUSEHOLD_PHONE_ID,
    PHONE_EXT,
    CLIENT_ID,
    CAN_CONTACT,
    COUNTRY_CODE,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("household_phone") }}


{% endsnapshot %}
