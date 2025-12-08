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
        unique_key=['_FIVETRAN_ID', 'PAP_CODE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'PAP_MASTER') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_pap_master',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='PAP_MASTER',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'PAP_MASTER') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'PAP_MASTER', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'PAP_MASTER') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'PAP_MASTER', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'PAP_MASTER') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'PAP_MASTER') %}
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
{{ transform_string('PAP_CODE') }} AS PAP_CODE,
{{ transform_string('PAP_CODE') }} AS PAP_CODE,
{{ transform_numeric('PAP_ID') }} AS PAP_ID,
{{ transform_numeric('PAP_ID') }} AS PAP_ID,
{{ transform_numeric('PRIORITY') }} AS PRIORITY,
{{ transform_numeric('PRIORITY') }} AS PRIORITY,
{{ transform_string('PROGRAM_CURRENCY') }} AS PROGRAM_CURRENCY,
{{ transform_string('PROGRAM_CURRENCY') }} AS PROGRAM_CURRENCY,
{{ transform_string('AGENCY_DOMESTIC') }} AS AGENCY_DOMESTIC,
{{ transform_string('AGENCY_DOMESTIC') }} AS AGENCY_DOMESTIC,
{{ transform_string('PAP_TYPE') }} AS PAP_TYPE,
{{ transform_string('PAP_TYPE') }} AS PAP_TYPE,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_string('PAP_NAME') }} AS PAP_NAME,
{{ transform_string('PAP_NAME') }} AS PAP_NAME,
{{ transform_string('EXTERNAL_RULES') }} AS EXTERNAL_RULES,
{{ transform_string('EXTERNAL_RULES') }} AS EXTERNAL_RULES,
{{ transform_string('IS_PRICE_PROGRAM') }} AS IS_PRICE_PROGRAM,
{{ transform_string('IS_PRICE_PROGRAM') }} AS IS_PRICE_PROGRAM,
{{ transform_string('PROMO_GROUP') }} AS PROMO_GROUP,
{{ transform_string('PROMO_GROUP') }} AS PROMO_GROUP,
{{ transform_string('COUPON_CLASS') }} AS COUPON_CLASS,
{{ transform_string('COUPON_CLASS') }} AS COUPON_CLASS,
{{ transform_string('PRINT_CRUISE_FARE_AMOUNT') }} AS PRINT_CRUISE_FARE_AMOUNT,
{{ transform_string('PRINT_CRUISE_FARE_AMOUNT') }} AS PRINT_CRUISE_FARE_AMOUNT,
NULL AS MANUAL_INPUT_VALUE_TYPE,
NULL AS MANUAL_INPUT_VALUE_TYPE,
NULL AS IS_ACTIVE,
NULL AS IS_ACTIVE,
NULL AS BEST_FARE_ELIGIBLE,
NULL AS BEST_FARE_ELIGIBLE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW1', 'PAP_MASTER') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_string('PAP_CODE') }} AS PAP_CODE,
{{ transform_string('PAP_CODE') }} AS PAP_CODE,
{{ transform_numeric('PAP_ID') }} AS PAP_ID,
{{ transform_numeric('PAP_ID') }} AS PAP_ID,
{{ transform_numeric('PRIORITY') }} AS PRIORITY,
{{ transform_numeric('PRIORITY') }} AS PRIORITY,
{{ transform_string('PROGRAM_CURRENCY') }} AS PROGRAM_CURRENCY,
{{ transform_string('PROGRAM_CURRENCY') }} AS PROGRAM_CURRENCY,
{{ transform_string('AGENCY_DOMESTIC') }} AS AGENCY_DOMESTIC,
{{ transform_string('AGENCY_DOMESTIC') }} AS AGENCY_DOMESTIC,
{{ transform_string('PAP_TYPE') }} AS PAP_TYPE,
{{ transform_string('PAP_TYPE') }} AS PAP_TYPE,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_string('PAP_NAME') }} AS PAP_NAME,
{{ transform_string('PAP_NAME') }} AS PAP_NAME,
{{ transform_string('EXTERNAL_RULES') }} AS EXTERNAL_RULES,
{{ transform_string('EXTERNAL_RULES') }} AS EXTERNAL_RULES,
{{ transform_string('IS_PRICE_PROGRAM') }} AS IS_PRICE_PROGRAM,
{{ transform_string('IS_PRICE_PROGRAM') }} AS IS_PRICE_PROGRAM,
{{ transform_string('PROMO_GROUP') }} AS PROMO_GROUP,
{{ transform_string('PROMO_GROUP') }} AS PROMO_GROUP,
{{ transform_string('COUPON_CLASS') }} AS COUPON_CLASS,
{{ transform_string('COUPON_CLASS') }} AS COUPON_CLASS,
{{ transform_string('PRINT_CRUISE_FARE_AMOUNT') }} AS PRINT_CRUISE_FARE_AMOUNT,
{{ transform_string('PRINT_CRUISE_FARE_AMOUNT') }} AS PRINT_CRUISE_FARE_AMOUNT,
{{ transform_string('MANUAL_INPUT_VALUE_TYPE') }} AS MANUAL_INPUT_VALUE_TYPE,
{{ transform_string('MANUAL_INPUT_VALUE_TYPE') }} AS MANUAL_INPUT_VALUE_TYPE,
{{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
{{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
{{ transform_string('BEST_FARE_ELIGIBLE') }} AS BEST_FARE_ELIGIBLE,
{{ transform_string('BEST_FARE_ELIGIBLE') }} AS BEST_FARE_ELIGIBLE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW2', 'PAP_MASTER') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
