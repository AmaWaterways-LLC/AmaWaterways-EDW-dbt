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
        unique_key=['RECORD_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'RES_ARCHIVE') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_res_archive',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='RES_ARCHIVE',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'RES_ARCHIVE') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'RES_ARCHIVE', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'RES_ARCHIVE') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'RES_ARCHIVE', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'RES_ARCHIVE') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'RES_ARCHIVE') %}
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
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
{{ transform_datetime('RES_INIT_DATE') }} AS RES_INIT_DATE,
{{ transform_numeric('RES_GUEST_COUNT') }} AS RES_GUEST_COUNT,
{{ transform_numeric('RES_ADULT_COUNT') }} AS RES_ADULT_COUNT,
{{ transform_numeric('RES_CHILD_COUNT') }} AS RES_CHILD_COUNT,
{{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
{{ transform_string('CABIN_CATEGORY') }} AS CABIN_CATEGORY,
{{ transform_string('CABIN_CATEGORY_BERTH') }} AS CABIN_CATEGORY_BERTH,
{{ transform_string('CABIN_NUMBER') }} AS CABIN_NUMBER,
{{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
{{ transform_datetime('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
{{ transform_datetime('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
{{ transform_string('PACKAGE_CODE') }} AS PACKAGE_CODE,
{{ transform_datetime('VACATION_DATE') }} AS VACATION_DATE,
{{ transform_datetime('VACATION_END') }} AS VACATION_END,
{{ transform_string('AIR_REQUEST_TYPE') }} AS AIR_REQUEST_TYPE,
{{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
{{ transform_numeric('RES_AMOUNT_TOTAL') }} AS RES_AMOUNT_TOTAL,
{{ transform_numeric('RES_DISCOUNT_TOTAL') }} AS RES_DISCOUNT_TOTAL,
{{ transform_numeric('COMMISSION_TOTAL') }} AS COMMISSION_TOTAL,
{{ transform_numeric('AMOUNT_SPENT_ONBOARD') }} AS AMOUNT_SPENT_ONBOARD,
{{ transform_string('RES_STATUS') }} AS RES_STATUS,
{{ transform_string('INSURANCE') }} AS INSURANCE,
{{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
{{ transform_string('SOURCE_CODE') }} AS SOURCE_CODE,
{{ transform_string('MANUAL_INPUT') }} AS MANUAL_INPUT,
{{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
{{ transform_string('TRAVEL_AGENT_PHONE_NBR') }} AS TRAVEL_AGENT_PHONE_NBR,
NULL AS AGENCY_TYPE,
{{ transform_string('ALT_RES_ID') }} AS ALT_RES_ID,
NULL AS ROUTE_ATTRIBUTE,
NULL AS PROMO_CODE,
NULL AS SHIP_OWNER_CODE,
NULL AS PORT_FROM,
NULL AS PORT_TO,
NULL AS OUTBOUND_OR_RETURN,
NULL AS GUEST_TYPE,
NULL AS CABIN_NUMBER_BOOKED,
NULL AS AGENT_ID,
NULL AS CRUISE_REVENUE_SCORE,
NULL AS CASINO_REVENUE,
NULL AS SAIL_DAYS,
NULL AS LAND_DAYS,
NULL AS LEGACY_POINTS,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW1', 'RES_ARCHIVE') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
{{ transform_datetime('RES_INIT_DATE') }} AS RES_INIT_DATE,
{{ transform_numeric('RES_GUEST_COUNT') }} AS RES_GUEST_COUNT,
{{ transform_numeric('RES_ADULT_COUNT') }} AS RES_ADULT_COUNT,
{{ transform_numeric('RES_CHILD_COUNT') }} AS RES_CHILD_COUNT,
{{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
{{ transform_string('CABIN_CATEGORY') }} AS CABIN_CATEGORY,
{{ transform_string('CABIN_CATEGORY_BERTH') }} AS CABIN_CATEGORY_BERTH,
{{ transform_string('CABIN_NUMBER') }} AS CABIN_NUMBER,
{{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
{{ transform_datetime('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
{{ transform_datetime('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
{{ transform_string('PACKAGE_CODE') }} AS PACKAGE_CODE,
{{ transform_datetime('VACATION_DATE') }} AS VACATION_DATE,
{{ transform_datetime('VACATION_END') }} AS VACATION_END,
{{ transform_string('AIR_REQUEST_TYPE') }} AS AIR_REQUEST_TYPE,
{{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
{{ transform_numeric('RES_AMOUNT_TOTAL') }} AS RES_AMOUNT_TOTAL,
{{ transform_numeric('RES_DISCOUNT_TOTAL') }} AS RES_DISCOUNT_TOTAL,
{{ transform_numeric('COMMISSION_TOTAL') }} AS COMMISSION_TOTAL,
{{ transform_numeric('AMOUNT_SPENT_ONBOARD') }} AS AMOUNT_SPENT_ONBOARD,
{{ transform_string('RES_STATUS') }} AS RES_STATUS,
{{ transform_string('INSURANCE') }} AS INSURANCE,
{{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
{{ transform_string('SOURCE_CODE') }} AS SOURCE_CODE,
{{ transform_string('MANUAL_INPUT') }} AS MANUAL_INPUT,
{{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
{{ transform_string('TRAVEL_AGENT_PHONE_NBR') }} AS TRAVEL_AGENT_PHONE_NBR,
{{ transform_string('AGENCY_TYPE') }} AS AGENCY_TYPE,
{{ transform_string('ALT_RES_ID') }} AS ALT_RES_ID,
{{ transform_string('ROUTE_ATTRIBUTE') }} AS ROUTE_ATTRIBUTE,
{{ transform_string('PROMO_CODE') }} AS PROMO_CODE,
{{ transform_string('SHIP_OWNER_CODE') }} AS SHIP_OWNER_CODE,
{{ transform_string('PORT_FROM') }} AS PORT_FROM,
{{ transform_string('PORT_TO') }} AS PORT_TO,
{{ transform_string('OUTBOUND_OR_RETURN') }} AS OUTBOUND_OR_RETURN,
{{ transform_string('GUEST_TYPE') }} AS GUEST_TYPE,
{{ transform_string('CABIN_NUMBER_BOOKED') }} AS CABIN_NUMBER_BOOKED,
{{ transform_numeric('AGENT_ID') }} AS AGENT_ID,
{{ transform_string('CRUISE_REVENUE_SCORE') }} AS CRUISE_REVENUE_SCORE,
{{ transform_numeric('CASINO_REVENUE') }} AS CASINO_REVENUE,
{{ transform_numeric('SAIL_DAYS') }} AS SAIL_DAYS,
{{ transform_numeric('LAND_DAYS') }} AS LAND_DAYS,
{{ transform_numeric('LEGACY_POINTS') }} AS LEGACY_POINTS,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW2', 'RES_ARCHIVE') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
