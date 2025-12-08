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
        unique_key=['_FIVETRAN_ID', 'RES_ADDON_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'RES_ADDON') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_res_addon',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='RES_ADDON',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'RES_ADDON') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'RES_ADDON', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'RES_ADDON') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'RES_ADDON', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'RES_ADDON') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'RES_ADDON') %}
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
{{ transform_numeric('RES_ADDON_ID') }} AS RES_ADDON_ID,
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_string('RES_ADDON_CODE') }} AS RES_ADDON_CODE,
{{ transform_datetime('START_DATE') }} AS START_DATE,
{{ transform_datetime('END_DATE') }} AS END_DATE,
{{ transform_datetime('EFFECTIVE_DATE') }} AS EFFECTIVE_DATE,
{{ transform_string('IS_DEFAULT') }} AS IS_DEFAULT,
{{ transform_string('PAP_CODE') }} AS PAP_CODE,
{{ transform_string('DELIVERY_PLACE') }} AS DELIVERY_PLACE,
{{ transform_numeric('QUANTITY') }} AS QUANTITY,
NULL AS IS_AUTO,
NULL AS AMENITY_SUBCODE,
NULL AS COMMENTS,
NULL AS DELIVERY_PLACE_DETAIL,
NULL AS HOTEL_ROOM_REQUEST_ID,
NULL AS SHIPROOM_REQUEST_ID,
NULL AS DINING_REQUEST_ID,
NULL AS LINK_FROM_REQUEST,
NULL AS ATTACHMENT_ID,
NULL AS ADDON_STATUS,
NULL AS PARENT_DESCR,
NULL AS ADDON_PRICE_CODE,
NULL AS PAID_DATE,
NULL AS LINK_DESCRIPTION,
NULL AS NOTES,
NULL AS MANDATORY_GROUP,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
{{ transform_numeric('RES_PACKAGE_ID') }} AS RES_PACKAGE_ID,
{{ transform_numeric('AM_ORDER_ID') }} AS AM_ORDER_ID,
{{ transform_string('AMENITY_CODE') }} AS AMENITY_CODE
    from {{ source('AMA_PROD_BRNZ_SW1', 'RES_ADDON') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('RES_ADDON_ID') }} AS RES_ADDON_ID,
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_string('RES_ADDON_CODE') }} AS RES_ADDON_CODE,
{{ transform_datetime('START_DATE') }} AS START_DATE,
{{ transform_datetime('END_DATE') }} AS END_DATE,
{{ transform_datetime('EFFECTIVE_DATE') }} AS EFFECTIVE_DATE,
{{ transform_string('IS_DEFAULT') }} AS IS_DEFAULT,
{{ transform_string('PAP_CODE') }} AS PAP_CODE,
{{ transform_string('DELIVERY_PLACE') }} AS DELIVERY_PLACE,
{{ transform_numeric('QUANTITY') }} AS QUANTITY,
{{ transform_string('IS_AUTO') }} AS IS_AUTO,
{{ transform_string('AMENITY_SUBCODE') }} AS AMENITY_SUBCODE,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_string('DELIVERY_PLACE_DETAIL') }} AS DELIVERY_PLACE_DETAIL,
{{ transform_numeric('HOTEL_ROOM_REQUEST_ID') }} AS HOTEL_ROOM_REQUEST_ID,
{{ transform_numeric('SHIPROOM_REQUEST_ID') }} AS SHIPROOM_REQUEST_ID,
{{ transform_numeric('DINING_REQUEST_ID') }} AS DINING_REQUEST_ID,
{{ transform_string('LINK_FROM_REQUEST') }} AS LINK_FROM_REQUEST,
{{ transform_numeric('ATTACHMENT_ID') }} AS ATTACHMENT_ID,
{{ transform_string('ADDON_STATUS') }} AS ADDON_STATUS,
{{ transform_string('PARENT_DESCR') }} AS PARENT_DESCR,
{{ transform_string('ADDON_PRICE_CODE') }} AS ADDON_PRICE_CODE,
{{ transform_datetime('PAID_DATE') }} AS PAID_DATE,
{{ transform_string('LINK_DESCRIPTION') }} AS LINK_DESCRIPTION,
{{ transform_string('NOTES') }} AS NOTES,
{{ transform_string('MANDATORY_GROUP') }} AS MANDATORY_GROUP,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
NULL AS RES_PACKAGE_ID,
NULL AS AM_ORDER_ID,
NULL AS AMENITY_CODE
    from {{ source('AMA_PROD_BRNZ_SW2', 'RES_ADDON') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
