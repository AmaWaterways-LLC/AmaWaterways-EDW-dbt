{% snapshot dim_res_header%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['RES_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_res_header_id",
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
    AGENCY_ID,
    AGENT_ID,
    CABIN_CATEGORY,
    RES_STATUS,
    GROUP_ID,
    DELEGATE_HEADER_ID,
    TERMINATION_COMPLETED,
    SHIP_CODE,
    PACKAGE_TYPE,
    SAIL_DATE_FROM,
    SAIL_DATE_TO,
    REL_DAY_FROM,
    REL_DAY_TO,
    SOURCE_CODE,
    SEC_AGENCY_ID,
    OFFICE_CODE,
    CURRENCY_RATE,
    CURRENCY_CODE,
    RH_ENTITY_TYPE,
    CANCELLATION_CASE,
    OFFICE_LOCATION,
    DEP_REF_ID,
    ARR_REF_ID,
    SEC_AGENT_ID,
    COMPANY_ID,
    COMPANY_AGENT_ID
    CONTACT_ID,
    RES_TYPE,
    PARENT_RES_ID,
    OWNERSHIP_CODE,
    VT_PACKAGE_ID,
    LAST_UPDATED_AT,
    IS_PROB_MANUAL,
    RES_MODE,
    RES_VERSION,
    ALLOW_INVOICING,
    ORIGIN_COUNTRY_CODE,
    ORIGINAL_INIT_DATE,
    CONFIRMATION_DATE,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("res_header") }}


{% endsnapshot %}
