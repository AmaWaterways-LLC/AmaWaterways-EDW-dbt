{# ================================================================
   Generate Batch ID (must be top-of-file, before config)
   ================================================================ #}
{% set batch_id = invocation_id ~ '-' ~ this.name ~ '-' ~ modules.datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S%fZ') %}

{# ================================================================
   CONFIG BLOCK
   ================================================================ #}
{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key=['_FIVETRAN_ID', 'RECORD_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'GROUP_HOTEL_REQUEST') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_group_hotel_request',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='GROUP_HOTEL_REQUEST',
                     layer='SILVER',
                     operation_type='MERGE',
                     load_type=load_type_val,
                     environment=target.name,
                     ingested_by='dbt',
                     batch_id='" ~ batch_id ~ "'
                 ) %}              
             {% endif %}"
        ],
        post_hook=[
            "{% do audit_update_counts(batch_id='" ~ batch_id ~ "', record_count_target=get_record_count(this)) %}",
            "{% do audit_end(batch_id='" ~ batch_id ~ "', status='SUCCESS') %}",
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'GROUP_HOTEL_REQUEST') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'GROUP_HOTEL_REQUEST', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'GROUP_HOTEL_REQUEST') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'GROUP_HOTEL_REQUEST', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'GROUP_HOTEL_REQUEST') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'GROUP_HOTEL_REQUEST') %}
    {% set wm_col_sw2 = cfg_sw2['WATERMARK_COLUMN'] %}
    {% set last_wm_sw2 = cfg_sw2['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw2 = (last_wm is none) %}
{% else %}
    {% set wm_col_sw1 = none %}
    {% set last_wm_sw1 = none %}
    {% set is_full_sw1 = true %}
    {% set wm_col_sw2 = none %}
    {% set last_wm_sw2 = none %}
    {% set is_full_sw2 = true %}
{% endif %}

{# ================================================================
   SOURCE CTE
   ================================================================ #}

with sw1_src as (
    select
    'SW1' AS DATA_SOURCE,
{{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
{{ transform_string('PRODUCT_TYPE') }} AS PRODUCT_TYPE,
{{ transform_string('HOTEL_REQUEST_TYPE') }} AS HOTEL_REQUEST_TYPE,
{{ transform_string('HOTEL_CATEGORY') }} AS HOTEL_CATEGORY,
{{ transform_string('HOTEL_ROOM_CATEGORY') }} AS HOTEL_ROOM_CATEGORY,
{{ transform_numeric('HOTEL_ID') }} AS HOTEL_ID,
{{ transform_string('HOTEL_ROOM_TYPE') }} AS HOTEL_ROOM_TYPE,
{{ transform_numeric('QUANTITY') }} AS QUANTITY,
{{ transform_numeric('N_OF_GUESTS') }} AS N_OF_GUESTS,
{{ transform_string('CABIN_CATEGORY') }} AS CABIN_CATEGORY,
{{ transform_string('PACKAGE_TYPE') }} AS PACKAGE_TYPE,
{{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
{{ transform_string('CITY_CODE') }} AS CITY_CODE,
{{ transform_datetime('STAY_DATE_FROM') }} AS STAY_DATE_FROM,
{{ transform_datetime('STAY_DATE_TO') }} AS STAY_DATE_TO,
{{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
{{ transform_datetime('EFFECTIVE_DATE') }} AS EFFECTIVE_DATE,
{{ transform_numeric('PACKAGE_ID') }} AS PACKAGE_ID,
NULL AS HOTEL_SPACE_TYPE,
NULL AS OCCUPANCY,
NULL AS TOUR_PACKAGE_ID,
NULL AS TOUR_PACKAGE_ITIN_ID,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW1', 'GROUP_HOTEL_REQUEST') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
{{ transform_string('PRODUCT_TYPE') }} AS PRODUCT_TYPE,
{{ transform_string('HOTEL_REQUEST_TYPE') }} AS HOTEL_REQUEST_TYPE,
{{ transform_string('HOTEL_CATEGORY') }} AS HOTEL_CATEGORY,
{{ transform_string('HOTEL_ROOM_CATEGORY') }} AS HOTEL_ROOM_CATEGORY,
{{ transform_numeric('HOTEL_ID') }} AS HOTEL_ID,
{{ transform_string('HOTEL_ROOM_TYPE') }} AS HOTEL_ROOM_TYPE,
{{ transform_numeric('QUANTITY') }} AS QUANTITY,
{{ transform_numeric('N_OF_GUESTS') }} AS N_OF_GUESTS,
{{ transform_string('CABIN_CATEGORY') }} AS CABIN_CATEGORY,
{{ transform_string('PACKAGE_TYPE') }} AS PACKAGE_TYPE,
{{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
{{ transform_string('CITY_CODE') }} AS CITY_CODE,
{{ transform_datetime('STAY_DATE_FROM') }} AS STAY_DATE_FROM,
{{ transform_datetime('STAY_DATE_TO') }} AS STAY_DATE_TO,
{{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
{{ transform_datetime('EFFECTIVE_DATE') }} AS EFFECTIVE_DATE,
{{ transform_numeric('PACKAGE_ID') }} AS PACKAGE_ID,
{{ transform_string('HOTEL_SPACE_TYPE') }} AS HOTEL_SPACE_TYPE,
{{ transform_numeric('OCCUPANCY') }} AS OCCUPANCY,
{{ transform_numeric('TOUR_PACKAGE_ID') }} AS TOUR_PACKAGE_ID,
{{ transform_numeric('TOUR_PACKAGE_ITIN_ID') }} AS TOUR_PACKAGE_ITIN_ID,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW2', 'GROUP_HOTEL_REQUEST') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
