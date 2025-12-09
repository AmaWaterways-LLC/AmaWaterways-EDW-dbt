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
        unique_key=['AGENT_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'AGENT') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_agent',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='AGENT',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'AGENT') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'AGENT', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'AGENT') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'AGENT', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'AGENT') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'AGENT') %}
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
{{ transform_numeric('AGENT_ID') }} AS AGENT_ID,
{{ transform_string('LAST_NAME') }} AS LAST_NAME,
{{ transform_string('FIRST_NAME') }} AS FIRST_NAME,
{{ transform_string('MIDDLE_NAME') }} AS MIDDLE_NAME,
{{ transform_string('FULL_NAME') }} AS FULL_NAME,
{{ transform_string('SALUTATION') }} AS SALUTATION,
{{ transform_string('TITLE') }} AS TITLE,
{{ transform_string('SEX') }} AS SEX,
{{ transform_datetime('BIRTHDAY') }} AS BIRTHDAY,
{{ transform_string('IATAN_NUMBER') }} AS IATAN_NUMBER,
{{ transform_string('IATAN_COMPANY_NAME') }} AS IATAN_COMPANY_NAME,
{{ transform_string('IATAN_HOLDER_NAME') }} AS IATAN_HOLDER_NAME,
{{ transform_string('IATAN_COMPANY_NUMBER') }} AS IATAN_COMPANY_NUMBER,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_datetime('IATAN_EXPIRATION_DATE') }} AS IATAN_EXPIRATION_DATE,
{{ transform_string('EMAIL') }} AS EMAIL,
{{ transform_string('IS_DISTRICT_SM') }} AS IS_DISTRICT_SM,
{{ transform_string('IS_INCENTIVE_SM') }} AS IS_INCENTIVE_SM,
{{ transform_string('WEB_PASSWORD') }} AS WEB_PASSWORD,
{{ transform_string('WEB_LOGIN_NAME') }} AS WEB_LOGIN_NAME,
{{ transform_datetime('WEB_LAST_LOGIN') }} AS WEB_LAST_LOGIN,
{{ transform_string('WEB_CANACCESAGENCYDATA') }} AS WEB_CANACCESAGENCYDATA,
{{ transform_string('ALLOW_WEB_ACCESS') }} AS ALLOW_WEB_ACCESS,
NULL AS SW_LOGIN_NAME,
NULL AS NOTIF_DFLT_DISTR_TYPE,
NULL AS FIRST_NAME_NATIVE,
NULL AS MIDDLE_NAME_NATIVE,
NULL AS LAST_NAME_NATIVE,
NULL AS WEB_LOGIN_FAILURES,
NULL AS ALT_AGENT_ID,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW1', 'AGENT') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('AGENT_ID') }} AS AGENT_ID,
{{ transform_string('LAST_NAME') }} AS LAST_NAME,
{{ transform_string('FIRST_NAME') }} AS FIRST_NAME,
{{ transform_string('MIDDLE_NAME') }} AS MIDDLE_NAME,
{{ transform_string('FULL_NAME') }} AS FULL_NAME,
{{ transform_string('SALUTATION') }} AS SALUTATION,
{{ transform_string('TITLE') }} AS TITLE,
{{ transform_string('SEX') }} AS SEX,
{{ transform_datetime('BIRTHDAY') }} AS BIRTHDAY,
{{ transform_string('IATAN_NUMBER') }} AS IATAN_NUMBER,
{{ transform_string('IATAN_COMPANY_NAME') }} AS IATAN_COMPANY_NAME,
{{ transform_string('IATAN_HOLDER_NAME') }} AS IATAN_HOLDER_NAME,
{{ transform_string('IATAN_COMPANY_NUMBER') }} AS IATAN_COMPANY_NUMBER,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_datetime('IATAN_EXPIRATION_DATE') }} AS IATAN_EXPIRATION_DATE,
{{ transform_string('EMAIL') }} AS EMAIL,
{{ transform_string('IS_DISTRICT_SM') }} AS IS_DISTRICT_SM,
{{ transform_string('IS_INCENTIVE_SM') }} AS IS_INCENTIVE_SM,
{{ transform_string('WEB_PASSWORD') }} AS WEB_PASSWORD,
{{ transform_string('WEB_LOGIN_NAME') }} AS WEB_LOGIN_NAME,
{{ transform_datetime('WEB_LAST_LOGIN') }} AS WEB_LAST_LOGIN,
{{ transform_string('WEB_CANACCESAGENCYDATA') }} AS WEB_CANACCESAGENCYDATA,
{{ transform_string('ALLOW_WEB_ACCESS') }} AS ALLOW_WEB_ACCESS,
{{ transform_string('SW_LOGIN_NAME') }} AS SW_LOGIN_NAME,
{{ transform_string('NOTIF_DFLT_DISTR_TYPE') }} AS NOTIF_DFLT_DISTR_TYPE,
{{ transform_string('FIRST_NAME_NATIVE') }} AS FIRST_NAME_NATIVE,
{{ transform_string('MIDDLE_NAME_NATIVE') }} AS MIDDLE_NAME_NATIVE,
{{ transform_string('LAST_NAME_NATIVE') }} AS LAST_NAME_NATIVE,
{{ transform_numeric('WEB_LOGIN_FAILURES') }} AS WEB_LOGIN_FAILURES,
{{ transform_string('ALT_AGENT_ID') }} AS ALT_AGENT_ID,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW2', 'AGENT') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
