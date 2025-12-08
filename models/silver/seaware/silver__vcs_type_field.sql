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
        unique_key=['_FIVETRAN_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'VCS_TYPE_FIELD') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_vcs_type_field',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='VCS_TYPE_FIELD',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'VCS_TYPE_FIELD') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'VCS_TYPE_FIELD', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'VCS_TYPE_FIELD') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'VCS_TYPE_FIELD', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'VCS_TYPE_FIELD') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'VCS_TYPE_FIELD') %}
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
{{ transform_string('ROW_TYPE') }} AS ROW_TYPE,
{{ transform_string('FIELD_NAME') }} AS FIELD_NAME,
{{ transform_string('FIELD_TYPE') }} AS FIELD_TYPE,
{{ transform_string('FIELD_CATEGORY') }} AS FIELD_CATEGORY,
{{ transform_string('FIELD_LABEL') }} AS FIELD_LABEL,
{{ transform_string('REF_KEY_FIELDS') }} AS REF_KEY_FIELDS,
{{ transform_string('IS_NULL') }} AS IS_NULL,
{{ transform_numeric('ORDER_NUMBER') }} AS ORDER_NUMBER,
{{ transform_string('IS_PARAM_COMMON') }} AS IS_PARAM_COMMON,
{{ transform_string('PARAM_EDIT_MODE') }} AS PARAM_EDIT_MODE,
{{ transform_string('IS_MULTIPLE') }} AS IS_MULTIPLE,
{{ transform_string('IS_CASE_SENSITIVE') }} AS IS_CASE_SENSITIVE,
{{ transform_string('DELETE_WHEN_COPY') }} AS DELETE_WHEN_COPY,
{{ transform_string('DELETE_WHEN_REINSTATE') }} AS DELETE_WHEN_REINSTATE,
{{ transform_string('IS_VIRTUAL') }} AS IS_VIRTUAL,
{{ transform_string('RTTI_DOMAIN_FIELD') }} AS RTTI_DOMAIN_FIELD,
{{ transform_string('RTTI_DOMAINS') }} AS RTTI_DOMAINS,
{{ transform_string('CUSTOMER') }} AS CUSTOMER,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW1', 'VCS_TYPE_FIELD') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
{{ transform_string('ROW_TYPE') }} AS ROW_TYPE,
{{ transform_string('FIELD_NAME') }} AS FIELD_NAME,
{{ transform_string('FIELD_TYPE') }} AS FIELD_TYPE,
{{ transform_string('FIELD_CATEGORY') }} AS FIELD_CATEGORY,
{{ transform_string('FIELD_LABEL') }} AS FIELD_LABEL,
{{ transform_string('REF_KEY_FIELDS') }} AS REF_KEY_FIELDS,
{{ transform_string('IS_NULL') }} AS IS_NULL,
{{ transform_numeric('ORDER_NUMBER') }} AS ORDER_NUMBER,
{{ transform_string('IS_PARAM_COMMON') }} AS IS_PARAM_COMMON,
{{ transform_string('PARAM_EDIT_MODE') }} AS PARAM_EDIT_MODE,
{{ transform_string('IS_MULTIPLE') }} AS IS_MULTIPLE,
{{ transform_string('IS_CASE_SENSITIVE') }} AS IS_CASE_SENSITIVE,
{{ transform_string('DELETE_WHEN_COPY') }} AS DELETE_WHEN_COPY,
{{ transform_string('DELETE_WHEN_REINSTATE') }} AS DELETE_WHEN_REINSTATE,
{{ transform_string('IS_VIRTUAL') }} AS IS_VIRTUAL,
{{ transform_string('RTTI_DOMAIN_FIELD') }} AS RTTI_DOMAIN_FIELD,
{{ transform_string('RTTI_DOMAINS') }} AS RTTI_DOMAINS,
{{ transform_string('CUSTOMER') }} AS CUSTOMER,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW2', 'VCS_TYPE_FIELD') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
