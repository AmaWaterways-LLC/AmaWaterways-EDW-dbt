{% snapshot dim_ship_deck%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['SHIP_CODE','DECK_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_ship_cabin_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    SHIP_CODE,
    DECK_NUMBER,
    DECK_ID,
    DECK_NAME,
    IS_DECK_PAX,
    IMAGE_ID,
    DECK_CODE,
    LAYOUT_IMAGE_ID,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED
FROM {{ ref("ship_deck") }}


{% endsnapshot %}
