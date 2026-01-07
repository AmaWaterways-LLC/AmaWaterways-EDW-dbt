{% snapshot dim_agency%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['AGENCY_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_agency_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE                AS DATA_SOURCE,
    AGENCY_ID                  AS AGENCY_ID,
    AGENCY_NAME_TYPED           AS AGENCY_NAME_TYPED,
    AGENCY_NAME                 AS AGENCY_NAME,
    ARC_NUMBER                  AS ARC_NUMBER,
    IATA_NUMBER                 AS IATA_NUMBER,
    CLIA_NUMBER                 AS CLIA_NUMBER,
    IS_ACTIVE                   AS IS_ACTIVE,
    IS_DOMESTIC                 AS IS_DOMESTIC,
    IS_INTERNAL                 AS IS_INTERNAL,
    USE_GUEST_ADDR              AS USE_GUEST_ADDR,
    COMMENTS                    AS COMMENTS,
    MARKETING_MSG               AS MARKETING_MSG,
    IS_GSA                      AS IS_GSA,
    CONSORTIUM_TYPE             AS CONSORTIUM_TYPE,
    IS_CONSORTIUM               AS IS_CONSORTIUM,
    AGENCY_TYPE                 AS AGENCY_TYPE,
    DEFAULT_CURRENCY            AS DEFAULT_CURRENCY,
    VENDOR_NUMBER               AS VENDOR_NUMBER,
    USE_DFLT_CONSORTIUM         AS USE_DFLT_CONSORTIUM,
    BC_TYPE_ID                  AS BC_TYPE_ID,
    TAXPAYER_TYPE               AS TAXPAYER_TYPE,
    TAXPAYER_NUMBER             AS TAXPAYER_NUMBER,
    CONTACT_NAME                AS CONTACT_NAME,
    WEB_ADDRESS                 AS WEB_ADDRESS,
    AGENCY_NAME_ESSENTIAL       AS AGENCY_NAME_ESSENTIAL,
    FLOATING_DEPOSIT            AS FLOATING_DEPOSIT,
    LANGUAGE_CODE               AS LANGUAGE_CODE,
    OFFICE_CODE                 AS OFFICE_CODE,
    AG_ENTITY_TYPE              AS AG_ENTITY_TYPE,
    IS_FAX_EMAIL_SECURED        AS IS_FAX_EMAIL_SECURED,
    ABTA_NUMBER                 AS ABTA_NUMBER,
    NOTIF_DFLT_DISTR_TYPE       AS NOTIF_DFLT_DISTR_TYPE,
    TAXOWNER_NAME               AS TAXOWNER_NAME,
    INVOICING_NUMBER            AS INVOICING_NUMBER,
    AGENCY_CODE                 AS AGENCY_CODE,
    FPR_AT_GROSS                AS FPR_AT_GROSS,
    INVOICING_IS_STOPPED        AS INVOICING_IS_STOPPED,
    INVOICING_DELAY_UNTIL       AS INVOICING_DELAY_UNTIL,
    INVOICING_ERROR_MSG         AS INVOICING_ERROR_MSG,
    BALANCE_RECALC_ERROR_MSG    AS BALANCE_RECALC_ERROR_MSG,
    IS_WHITELABEL               AS IS_WHITELABEL,
    VALID_CURRENCIES            AS VALID_CURRENCIES,
    ALPHA_NUM_ID                AS ALPHA_NUM_ID,
    ALLOW_REG_COMMISS_TRANSACTIONS AS ALLOW_REG_COMMISS_TRANSACTIONS,
    ALT_AGENCY_ID               AS ALT_AGENCY_ID,
    LAST_UPDATED_TIMESTAMP      AS LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED              AS SOURCE_DELETED,
    COUNTRY_CODE                AS COUNTRY_CODE,
    GCPS_NUMBER                 AS GCPS_NUMBER,
    KEEP_SELF_COMMISSIONS       AS KEEP_SELF_COMMISSIONS,
    EMAIL                       AS EMAIL
FROM {{ ref("agency") }}


{% endsnapshot %}
