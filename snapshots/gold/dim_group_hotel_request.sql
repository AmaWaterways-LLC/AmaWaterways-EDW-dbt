{% snapshot dim_group_hotel_request%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['RECORD_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_group_hotel_request_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    RECORD_ID,
    PRODUCT_TYPE,
    HOTEL_REQUEST_TYPE,
    HOTEL_CATEGORY,
    HOTEL_ROOM_CATEGORY,
    HOTEL_ID,
    HOTEL_ROOM_TYPE,
    QUANTITY,
    N_OF_GUESTS,
    CABIN_CATEGORY,
    PACKAGE_TYPE,
    SHIP_CODE,
    CITY_CODE,
    STAY_DATE_FROM,
    STAY_DATE_TO,
    GROUP_ID,
    EFFECTIVE_DATE,
    PACKAGE_ID,
    OCCUPANCY,
    TOUR_PACKAGE_ID,
    TOUR_PACKAGE_ITIN_ID,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED,
FROM {{ ref("group_hotel_request") }}


{% endsnapshot %}
