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
        unique_key=['AGENCY_PHONE_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'AGENCY_PHONE') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_agency_phone',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='AGENCY_PHONE',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'AGENCY_PHONE') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'AGENCY_PHONE', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'AGENCY_PHONE') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'AGENCY_PHONE', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'AGENCY_PHONE') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'AGENCY_PHONE') %}
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
{{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
{{ transform_numeric('SEQ_NUMBER') }} AS SEQ_NUMBER,
{{ transform_string('PHONE_TYPE') }} AS PHONE_TYPE,
{{ transform_numeric('INTL_CODE') }} AS INTL_CODE,
{{ transform_string('PHONE_NUMBER') }} AS PHONE_NUMBER,
{{ transform_string('IS_PHONE_PRIMARY') }} AS IS_PHONE_PRIMARY,
{{ transform_string('IS_PHONE_SECONDARY') }} AS IS_PHONE_SECONDARY,
{{ transform_string('IS_PHONE_FAX') }} AS IS_PHONE_FAX,
{{ transform_string('FAX_ATTENTION_LINE') }} AS FAX_ATTENTION_LINE,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_numeric('AGENCY_PHONE_ID') }} AS AGENCY_PHONE_ID,
{{ transform_string('PHONE_EXT') }} AS PHONE_EXT,
NULL AS COUNTRY_CODE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW1', 'AGENCY_PHONE') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
{{ transform_numeric('SEQ_NUMBER') }} AS SEQ_NUMBER,
{{ transform_string('PHONE_TYPE') }} AS PHONE_TYPE,
{{ transform_numeric('INTL_CODE') }} AS INTL_CODE,
{{ transform_string('PHONE_NUMBER') }} AS PHONE_NUMBER,
{{ transform_string('IS_PHONE_PRIMARY') }} AS IS_PHONE_PRIMARY,
{{ transform_string('IS_PHONE_SECONDARY') }} AS IS_PHONE_SECONDARY,
{{ transform_string('IS_PHONE_FAX') }} AS IS_PHONE_FAX,
{{ transform_string('FAX_ATTENTION_LINE') }} AS FAX_ATTENTION_LINE,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_numeric('AGENCY_PHONE_ID') }} AS AGENCY_PHONE_ID,
{{ transform_string('PHONE_EXT') }} AS PHONE_EXT,
{{ transform_string('COUNTRY_CODE') }} AS COUNTRY_CODE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW2', 'AGENCY_PHONE') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
