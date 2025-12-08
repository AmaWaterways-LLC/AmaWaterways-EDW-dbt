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
        unique_key=['AIR_REQUEST_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'RES_AIR_REQUEST') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_res_air_request',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='RES_AIR_REQUEST',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'RES_AIR_REQUEST') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'RES_AIR_REQUEST', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'RES_AIR_REQUEST') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'RES_AIR_REQUEST', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'RES_AIR_REQUEST') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'RES_AIR_REQUEST') %}
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
    {{ transform_numeric('AIR_REQUEST_ID') }} AS AIR_REQUEST_ID,
    {{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
    {{ transform_string('REQUEST_TYPE') }} AS REQUEST_TYPE,
    {{ transform_datetime('DATE_OUT') }} AS DATE_OUT,
    {{ transform_string('GATEWAY_OUT') }} AS GATEWAY_OUT,
    {{ transform_string('AIR_FLIGHT_TIMING_OUT') }} AS AIR_FLIGHT_TIMING_OUT,
    {{ transform_string('DESTINATION_CODE') }} AS DESTINATION_CODE,
    {{ transform_string('DESTINATION_RETURN') }} AS DESTINATION_RETURN,
    {{ transform_datetime('DATE_RETURN') }} AS DATE_RETURN,
    {{ transform_string('GATEWAY_RETURN') }} AS GATEWAY_RETURN,
    {{ transform_string('AIR_FLIGHT_TIMING_RETURN') }} AS AIR_FLIGHT_TIMING_RETURN,
    {{ transform_string('SEND_STATUS') }} AS SEND_STATUS,
    {{ transform_string('READY_STATUS') }} AS READY_STATUS,
    {{ transform_string('ACTIVE') }} AS ACTIVE,
    {{ transform_string('SEAWARE_RESPONSE') }} AS SEAWARE_RESPONSE,
    {{ transform_string('AIRWARE_RESPONSE') }} AS AIRWARE_RESPONSE,
    {{ transform_numeric('REQ_ROUTING_SEQUENCE') }} AS REQ_ROUTING_SEQUENCE,
    {{ transform_numeric('NUMBER_OF_SEATS') }} AS NUMBER_OF_SEATS,
    {{ transform_string('SEAT_TYPE') }} AS SEAT_TYPE,
    {{ transform_string('SEAT_LOCATION') }} AS SEAT_LOCATION,
    {{ transform_string('CLOS') }} AS CLOS,
    {{ transform_string('INVENTORY_REQUEST_TYPE') }} AS INVENTORY_REQUEST_TYPE,
    {{ transform_numeric('ALLOCATION_ID') }} AS ALLOCATION_ID,
    {{ transform_datetime('EFFECTIVE_DATE') }} AS EFFECTIVE_DATE,
    {{ transform_numeric('GNRC_ITIN_ID') }} AS GNRC_ITIN_ID,
    {{ transform_numeric('XML_REQUEST_ID') }} AS XML_REQUEST_ID,
    {{ transform_string('PNR') }} AS PNR,
    {{ transform_numeric('PA_DEVIATION_REQ_ID') }} AS PA_DEVIATION_REQ_ID,
    {{ transform_datetime('ITIN_SET_DATE') }} AS ITIN_SET_DATE,
    {{ transform_string('ITIN_PRICE_CURRENCY') }} AS ITIN_PRICE_CURRENCY,
    {{ transform_numeric('ITIN_PRICE_AMOUNT') }} AS ITIN_PRICE_AMOUNT,
    {{ transform_numeric('ITIN_PRICE_TAXES') }} AS ITIN_PRICE_TAXES,
    {{ transform_string('ITIN_COST_CURRENCY') }} AS ITIN_COST_CURRENCY,
    {{ transform_numeric('ITIN_COST_AMOUNT') }} AS ITIN_COST_AMOUNT,
    {{ transform_numeric('ITIN_COST_TAXES') }} AS ITIN_COST_TAXES,
    {{ transform_string('ESEAAIR_FLAG') }} AS ESEAAIR_FLAG,
     _FIVETRAN_DELETED AS SOURCE_DELETED,
    {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP
    from {{ source('AMA_PROD_BRNZ_SW1', 'RES_AIR_REQUEST') }}
        {% if is_incremental() and not is_full %}
        where coalesce({{ wm_col }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm) }}
        {% endif %}
)
select * 
from src
