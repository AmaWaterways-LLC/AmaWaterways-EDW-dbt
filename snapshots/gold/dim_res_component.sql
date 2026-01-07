{% snapshot dim_res_component%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['RES_COMPONENT_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_res_component_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    RES_COMPONENT_ID,
    EFFECTIVE_DATE,
    END_DATE,
    START_DATE,
    COMPONENT_CODE,
    RES_ID,
    COMPONENT_TYPE,
    GUEST_ID,
    RES_PACKAGE_ID,
    COMPONENT_SUBCODE1,
    COMPONENT_SUBCODE2,
    COMPONENT_SUBCODE3,
    LOCATION_TYPE_FROM,
    LOCATION_CODE_FROM,
    LOCATION_TYPE_TO,
    LOCATION_CODE_TO,
    ITIN_RECORD_ID,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("res_component") }}


{% endsnapshot %}
