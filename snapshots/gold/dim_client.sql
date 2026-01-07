{% snapshot dim_client%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['CLIENT_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_client_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    CLIENT_ID,
    HOUSEHOLD_ID,
    LAST_NAME,
    FIRST_NAME,
    MIDDLE_NAME,
    FULL_NAME,
    TITLE,
    IS_ACTIVE,
    BIRTHDAY,
    SEX,
    PASSPORT_NUMBER,
    PASSPORT_ISSUE_PLACE,
    PASSPORT_ISSUE_DATE,
    PASSPORT_EXP_DATE,
    CITIZENSHIP,
    CLIENT_TYPE,
    EMAIL,
    COUNTRY_OF_BIRTH,
    LANGUAGE_CODE,
    ALLOW_WEB_ACCESS,
    SEND_PROMOTIONAL_MAIL,
    SEND_PROMOTIONAL_EMAIL,
    SEND_PROMOTIONAL_SMS,
    HOUSEHOLD_ADDR_ID,
    CLIENT_GUID,
    WEB_LOGIN_FAILURES,
    SEND_PROMOTIONAL_PHONE,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("client") }}


{% endsnapshot %}
