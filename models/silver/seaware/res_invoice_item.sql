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
        unique_key=['RES_INVOICE_ITEM_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'RES_INVOICE_ITEM') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_res_invoice_item',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='RES_INVOICE_ITEM',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'RES_INVOICE_ITEM') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'RES_INVOICE_ITEM', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'RES_INVOICE_ITEM') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'RES_INVOICE_ITEM', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'RES_INVOICE_ITEM') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'RES_INVOICE_ITEM') %}
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
{{ transform_numeric('RES_INVOICE_ITEM_ID') }} AS RES_INVOICE_ITEM_ID,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_numeric('RES_PACKAGE_ID') }} AS RES_PACKAGE_ID,
{{ transform_string('INVOICE_ITEM_TYPE') }} AS INVOICE_ITEM_TYPE,
{{ transform_numeric('PRICE_PER_UNIT') }} AS PRICE_PER_UNIT,
{{ transform_numeric('N_OF_UNITS') }} AS N_OF_UNITS,
{{ transform_numeric('PERCENT') }} AS PERCENT,
{{ transform_numeric('AMOUNT') }} AS AMOUNT,
{{ transform_string('INVOICE_ITEM_SUBTYPE') }} AS INVOICE_ITEM_SUBTYPE,
{{ transform_string('INVOICE_ITEM_SUBTYPE2') }} AS INVOICE_ITEM_SUBTYPE2,
{{ transform_string('INVOICE_ITEM_SUBTYPE3') }} AS INVOICE_ITEM_SUBTYPE3,
{{ transform_string('PRICE_PROGRAM') }} AS PRICE_PROGRAM,
{{ transform_numeric('HOTEL_REQUEST_ID') }} AS HOTEL_REQUEST_ID,
{{ transform_numeric('COMMISSION_PERCENT') }} AS COMMISSION_PERCENT,
NULL AS PRICE_AREA,
NULL AS EXTENDED_INFO,
NULL AS ADJUSTMENT_ORIGIN,
NULL AS RES_PRODUCT_ID,
NULL AS CLIENT_ID,
NULL AS GUEST_SEQN,
NULL AS POINTS_BALANCE_ID,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW1', 'RES_INVOICE_ITEM') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('RES_INVOICE_ITEM_ID') }} AS RES_INVOICE_ITEM_ID,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_numeric('RES_PACKAGE_ID') }} AS RES_PACKAGE_ID,
{{ transform_string('INVOICE_ITEM_TYPE') }} AS INVOICE_ITEM_TYPE,
{{ transform_numeric('PRICE_PER_UNIT') }} AS PRICE_PER_UNIT,
{{ transform_numeric('N_OF_UNITS') }} AS N_OF_UNITS,
{{ transform_numeric('PERCENT') }} AS PERCENT,
{{ transform_numeric('AMOUNT') }} AS AMOUNT,
{{ transform_string('INVOICE_ITEM_SUBTYPE') }} AS INVOICE_ITEM_SUBTYPE,
{{ transform_string('INVOICE_ITEM_SUBTYPE2') }} AS INVOICE_ITEM_SUBTYPE2,
{{ transform_string('INVOICE_ITEM_SUBTYPE3') }} AS INVOICE_ITEM_SUBTYPE3,
{{ transform_string('PRICE_PROGRAM') }} AS PRICE_PROGRAM,
{{ transform_numeric('HOTEL_REQUEST_ID') }} AS HOTEL_REQUEST_ID,
{{ transform_numeric('COMMISSION_PERCENT') }} AS COMMISSION_PERCENT,
{{ transform_string('PRICE_AREA') }} AS PRICE_AREA,
{{ transform_string('EXTENDED_INFO') }} AS EXTENDED_INFO,
{{ transform_string('ADJUSTMENT_ORIGIN') }} AS ADJUSTMENT_ORIGIN,
{{ transform_numeric('RES_PRODUCT_ID') }} AS RES_PRODUCT_ID,
{{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
{{ transform_numeric('GUEST_SEQN') }} AS GUEST_SEQN,
{{ transform_numeric('POINTS_BALANCE_ID') }} AS POINTS_BALANCE_ID,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW2', 'RES_INVOICE_ITEM') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
