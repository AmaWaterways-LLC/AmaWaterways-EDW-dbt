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
        unique_key=['AM_ORDER_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'AM_ORDER_HEADER') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_am_order_header',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='AM_ORDER_HEADER',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'AM_ORDER_HEADER') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'AM_ORDER_HEADER', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'AM_ORDER_HEADER') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'AM_ORDER_HEADER', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'AM_ORDER_HEADER') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'AM_ORDER_HEADER') %}
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

with src as (
    select
    'SW1' AS DATA_SOURCE,
    {{ transform_numeric('AM_ORDER_ID') }} AS AM_ORDER_ID,
    {{ transform_datetime('ORDER_DATE') }} AS ORDER_DATE,
    {{ transform_string('ORDER_DESCRIPT') }} AS ORDER_DESCRIPT,
    {{ transform_string('ORD_STATUS_CODE') }} AS ORD_STATUS_CODE,
    {{ transform_string('AM_REQSTER_TYPE') }} AS AM_REQSTER_TYPE,
    {{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
    {{ transform_numeric('AGENT_ID') }} AS AGENT_ID,
    {{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
    {{ transform_string('AM_BOOKING_TYPE') }} AS AM_BOOKING_TYPE,
    {{ transform_numeric('RES_ID') }} AS RES_ID,
    {{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
    {{ transform_datetime('DISPATCHED_DATE') }} AS DISPATCHED_DATE,
    {{ transform_string('DISPATCHED_OK') }} AS DISPATCHED_OK,
    {{ transform_string('GIFT_CARD_TO') }} AS GIFT_CARD_TO,
    {{ transform_string('GIFT_CARD_MSG') }} AS GIFT_CARD_MSG,
    {{ transform_string('COMPLIMENTS_OF') }} AS COMPLIMENTS_OF,
    {{ transform_string('ORDER_VALID') }} AS ORDER_VALID,
    {{ transform_string('TERMINATION_COMPLETED') }} AS TERMINATION_COMPLETED,
    {{ transform_string('CREATED_FROM_ADDON') }} AS CREATED_FROM_ADDON,
    {{ transform_string('CANCEL_REFUND_DONE') }} AS CANCEL_REFUND_DONE,
    {{ transform_string('FREE_OF_CHARGE') }} AS FREE_OF_CHARGE,
    {{ transform_string('GROUP_DISTRIBUTION_OK') }} AS GROUP_DISTRIBUTION_OK,
    {{ transform_string('SHOW_ITEM_LIST') }} AS SHOW_ITEM_LIST,
    {{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
    {{ transform_string('AM_ENTITY_TYPE') }} AS AM_ENTITY_TYPE,
    {{ transform_string('INVOICE_VALID') }} AS INVOICE_VALID,
    {{ transform_string('FROM_COUPON') }} AS FROM_COUPON,
     _FIVETRAN_DELETED AS SOURCE_DELETED,
    {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP
    from {{ source('AMA_PROD_BRNZ_SW1', 'AM_ORDER_HEADER') }}
        {% if is_incremental() and not is_full %}
        where coalesce({{ wm_col }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm) }}
        {% endif %}
)
select * 
from src
