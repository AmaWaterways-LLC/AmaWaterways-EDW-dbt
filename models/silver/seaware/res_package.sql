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
        unique_key=['RES_PACKAGE_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'RES_PACKAGE') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_res_package',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='RES_PACKAGE',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'RES_PACKAGE') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'RES_PACKAGE', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'RES_PACKAGE') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'RES_PACKAGE', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'RES_PACKAGE') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'RES_PACKAGE') %}
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
{{ transform_numeric('RES_PACKAGE_ID') }} AS RES_PACKAGE_ID,
{{ transform_datetime('START_DATE') }} AS START_DATE,
{{ transform_datetime('END_DATE') }} AS END_DATE,
{{ transform_datetime('EFFECTIVE_DATE') }} AS EFFECTIVE_DATE,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_string('PACKAGE_TYPE') }} AS PACKAGE_TYPE,
{{ transform_numeric('PACKAGE_ID') }} AS PACKAGE_ID,
{{ transform_string('IS_PRICEABLE') }} AS IS_PRICEABLE,
NULL AS DEP_REF_ID,
NULL AS ARR_REF_ID,
NULL AS SHIP_CODE,
NULL AS CABIN_CATEGORY,
NULL AS CABIN_CATEGORY_BERTH,
NULL AS CABIN_SEQ_NUMBERS,
NULL AS PACKAGE_CLASS,
NULL AS HOTEL_REQUEST_ID,
NULL AS RES_ADDON_ID,
NULL AS DINING_REQUEST_ID,
NULL AS CAR_REQ_LEG_ID,
NULL AS SHIPROOM_REQUEST_ID,
NULL AS PACKAGE_STATUS,
NULL AS PPS_CARD_OWNER_ID,
NULL AS COMMENT_ID,
NULL AS IS_NO_SHOW,
NULL AS CAB_CTG_UPG_PROMO,
NULL AS PLACE_COUNT,
NULL AS PARENT_GUEST_ID,
NULL AS SRC_PACKAGE_ID,
NULL AS SRC_DEP_REF_ID,
NULL AS SRC_PACKAGE_CLASS,
NULL AS SRC_ARR_REF_ID,
NULL AS SRC_LINK_DESCRIPTION,
NULL AS RES_PRODUCT_ID,
NULL AS PLACE_COUNT_RESERVED,
NULL AS SOURCE_CODE,
NULL AS AIR_PRODUCT_ID,
NULL AS TRANSFER_REQUEST_ID,
{{ transform_numeric('EXTRA_GUEST_COUNT') }} AS EXTRA_GUEST_COUNT,
NULL AS RES_TOUR_ITEM_ID,
NULL AS EXT_COMP_ID,
NULL AS SELL_LIM_INV_RESULT,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
{{ transform_string('NEEDS_EXTRA_SEAT') }} AS NEEDS_EXTRA_SEAT
    from {{ source('AMA_PROD_BRNZ_SW1', 'RES_PACKAGE') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('RES_PACKAGE_ID') }} AS RES_PACKAGE_ID,
{{ transform_datetime('START_DATE') }} AS START_DATE,
{{ transform_datetime('END_DATE') }} AS END_DATE,
{{ transform_datetime('EFFECTIVE_DATE') }} AS EFFECTIVE_DATE,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_string('PACKAGE_TYPE') }} AS PACKAGE_TYPE,
{{ transform_numeric('PACKAGE_ID') }} AS PACKAGE_ID,
{{ transform_string('IS_PRICEABLE') }} AS IS_PRICEABLE,
{{ transform_numeric('DEP_REF_ID') }} AS DEP_REF_ID,
{{ transform_numeric('ARR_REF_ID') }} AS ARR_REF_ID,
{{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
{{ transform_string('CABIN_CATEGORY') }} AS CABIN_CATEGORY,
{{ transform_string('CABIN_CATEGORY_BERTH') }} AS CABIN_CATEGORY_BERTH,
{{ transform_string('CABIN_SEQ_NUMBERS') }} AS CABIN_SEQ_NUMBERS,
{{ transform_string('PACKAGE_CLASS') }} AS PACKAGE_CLASS,
{{ transform_numeric('HOTEL_REQUEST_ID') }} AS HOTEL_REQUEST_ID,
{{ transform_numeric('RES_ADDON_ID') }} AS RES_ADDON_ID,
{{ transform_numeric('DINING_REQUEST_ID') }} AS DINING_REQUEST_ID,
{{ transform_numeric('CAR_REQ_LEG_ID') }} AS CAR_REQ_LEG_ID,
{{ transform_numeric('SHIPROOM_REQUEST_ID') }} AS SHIPROOM_REQUEST_ID,
{{ transform_string('PACKAGE_STATUS') }} AS PACKAGE_STATUS,
{{ transform_numeric('PPS_CARD_OWNER_ID') }} AS PPS_CARD_OWNER_ID,
{{ transform_numeric('COMMENT_ID') }} AS COMMENT_ID,
{{ transform_string('IS_NO_SHOW') }} AS IS_NO_SHOW,
{{ transform_string('CAB_CTG_UPG_PROMO') }} AS CAB_CTG_UPG_PROMO,
{{ transform_numeric('PLACE_COUNT') }} AS PLACE_COUNT,
{{ transform_numeric('PARENT_GUEST_ID') }} AS PARENT_GUEST_ID,
{{ transform_numeric('SRC_PACKAGE_ID') }} AS SRC_PACKAGE_ID,
{{ transform_numeric('SRC_DEP_REF_ID') }} AS SRC_DEP_REF_ID,
{{ transform_string('SRC_PACKAGE_CLASS') }} AS SRC_PACKAGE_CLASS,
{{ transform_numeric('SRC_ARR_REF_ID') }} AS SRC_ARR_REF_ID,
{{ transform_string('SRC_LINK_DESCRIPTION') }} AS SRC_LINK_DESCRIPTION,
{{ transform_numeric('RES_PRODUCT_ID') }} AS RES_PRODUCT_ID,
{{ transform_numeric('PLACE_COUNT_RESERVED') }} AS PLACE_COUNT_RESERVED,
{{ transform_string('SOURCE_CODE') }} AS SOURCE_CODE,
{{ transform_numeric('AIR_PRODUCT_ID') }} AS AIR_PRODUCT_ID,
{{ transform_numeric('TRANSFER_REQUEST_ID') }} AS TRANSFER_REQUEST_ID,
{{ transform_numeric('EXTRA_GUEST_COUNT') }} AS EXTRA_GUEST_COUNT,
{{ transform_numeric('RES_TOUR_ITEM_ID') }} AS RES_TOUR_ITEM_ID,
{{ transform_numeric('EXT_COMP_ID') }} AS EXT_COMP_ID,
{{ transform_string('SELL_LIM_INV_RESULT') }} AS SELL_LIM_INV_RESULT,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
NULL AS NEEDS_EXTRA_SEAT
    from {{ source('AMA_PROD_BRNZ_SW2', 'RES_PACKAGE') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
