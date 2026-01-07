{% snapshot dim_sail_header%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['SAIL_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_sail_header_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    SAIL_ID,
    SHIP_CODE,
    SAIL_DATE_FROM,
    SAIL_DATE_TO,
    REL_DAY_FROM,
    REL_DAY_TO,
    SEASON_CODE,
    PORT_FROM,
    PORT_TO,
    GEOG_AREA_CODE,
    IS_FAKE,
    IS_ACTIVE,
    SAIL_STATUS,
    SAIL_CODE,
    IS_LOCKED,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("sail_header") }}


{% endsnapshot %}
