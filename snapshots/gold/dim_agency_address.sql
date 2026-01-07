{% snapshot dim_agency_address%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['AGENCY_ADDR_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_agency_address_id",
            "dbt_updated_at": "updated_at",
        }
    )
}}


SELECT
    DATA_SOURCE,
    AGENCY_ADDR_ID,
    AGENCY_ID,
    SEQ_NUMBER,
    ADDRESS_TYPE,
    ADDRESS_LINE1,
    ADDRESS_LINE2,
    ADDRESS_CITY,
    STATE_CODE,
    ZIP,
    COUNTRY_CODE,
    IS_ADDRESS_MAILING,
    IS_ADDRESS_SHIPPING,
    COMMENTS,
    ADDRESS_LINE3,
    ADDRESS_LINE4,
    ADDRESS_ESSENTIAL,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("agency_address") }}


{% endsnapshot %}