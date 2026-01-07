{% snapshot dim_package_definition%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['PACKAGE_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_package_definition_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    PACKAGE_ID,
    PACKAGE_CODE,
    VACATION_DATE,
    PACKAGE_TYPE,
    SAIL_ID,
    SEASON_CODE,
    GEOG_AREA_CODE,
    IS_ACTIVE,
    COMMENTS,
    SORT_SEQN,
    SHOREX_MODE,
    SHOREX_TIMING,
    INITIAL_STATUS,
    ROUTE_CODE,
    PACKAGE_NAME,
    SAIL_SEGMENTS,
    SAIL_DAYS_OVERRIDE,
    POINTS_SAIL_SEGMENTS,
    TOUR_STATUS,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("package_definition") }}


{% endsnapshot %}
