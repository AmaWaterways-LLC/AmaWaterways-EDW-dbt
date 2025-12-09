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
        unique_key=['COUNTRY_CODE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'COUNTRY') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_country',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='COUNTRY',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'COUNTRY') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'COUNTRY', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'COUNTRY') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'COUNTRY', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'COUNTRY') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'COUNTRY') %}
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
{{ transform_numeric('COUNTRY_ID') }} AS COUNTRY_ID,
{{ transform_string('COUNTRY_CODE') }} AS COUNTRY_CODE,
{{ transform_string('COUNTRY_NAME') }} AS COUNTRY_NAME,
{{ transform_string('COUNTRY_CODE2') }} AS COUNTRY_CODE2,
{{ transform_numeric('COUNTRY_ISO_ID') }} AS COUNTRY_ISO_ID,
{{ transform_numeric('INTL_CODE') }} AS INTL_CODE,
{{ transform_string('PHONE_INPUT_MASK') }} AS PHONE_INPUT_MASK,
{{ transform_string('POSTAL_CODE_MASK') }} AS POSTAL_CODE_MASK,
{{ transform_string('POSTAL_CODE_PROVIDED') }} AS POSTAL_CODE_PROVIDED,
{{ transform_string('POSTAL_CODE_COMPLETE') }} AS POSTAL_CODE_COMPLETE,
{{ transform_numeric('POSTAL_SYMBOLS_NUM_USED') }} AS POSTAL_SYMBOLS_NUM_USED,
NULL AS AGENCY_ID,
NULL AS COUNTRY_CODE3,
NULL AS POSTAL_CODE_REGEXP_MATCH,
NULL AS POSTAL_CODE_REGEXP_TRIM,
NULL AS POSTAL_CODE_FORMAT_SQL,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW1', 'COUNTRY') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('COUNTRY_ID') }} AS COUNTRY_ID,
{{ transform_string('COUNTRY_CODE') }} AS COUNTRY_CODE,
{{ transform_string('COUNTRY_NAME') }} AS COUNTRY_NAME,
{{ transform_string('COUNTRY_CODE2') }} AS COUNTRY_CODE2,
{{ transform_numeric('COUNTRY_ISO_ID') }} AS COUNTRY_ISO_ID,
{{ transform_numeric('INTL_CODE') }} AS INTL_CODE,
{{ transform_string('PHONE_INPUT_MASK') }} AS PHONE_INPUT_MASK,
{{ transform_string('POSTAL_CODE_MASK') }} AS POSTAL_CODE_MASK,
{{ transform_string('POSTAL_CODE_PROVIDED') }} AS POSTAL_CODE_PROVIDED,
{{ transform_string('POSTAL_CODE_COMPLETE') }} AS POSTAL_CODE_COMPLETE,
{{ transform_numeric('POSTAL_SYMBOLS_NUM_USED') }} AS POSTAL_SYMBOLS_NUM_USED,
{{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
{{ transform_string('COUNTRY_CODE3') }} AS COUNTRY_CODE3,
{{ transform_string('POSTAL_CODE_REGEXP_MATCH') }} AS POSTAL_CODE_REGEXP_MATCH,
{{ transform_string('POSTAL_CODE_REGEXP_TRIM') }} AS POSTAL_CODE_REGEXP_TRIM,
{{ transform_string('POSTAL_CODE_FORMAT_SQL') }} AS POSTAL_CODE_FORMAT_SQL,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW2', 'COUNTRY') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
