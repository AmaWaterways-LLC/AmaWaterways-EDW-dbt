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
        unique_key=['CC_ACC_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'CC_CLIENT') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_cc_client',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='CC_CLIENT',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'CC_CLIENT') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'CC_CLIENT', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'CC_CLIENT') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'CC_CLIENT', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'CC_CLIENT') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'CC_CLIENT') %}
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
{{ transform_string('CC_ACC_NUMBER') }} AS CC_ACC_NUMBER,
{{ transform_numeric('CC_ACC_ID') }} AS CC_ACC_ID,
{{ transform_datetime('CC_EXP_DATE') }} AS CC_EXP_DATE,
{{ transform_string('IS_CC_ACTIVE') }} AS IS_CC_ACTIVE,
{{ transform_string('IS_CC_BLACKLISTED') }} AS IS_CC_BLACKLISTED,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_string('CC_NAME_ON_CARD') }} AS CC_NAME_ON_CARD,
{{ transform_string('ADDRESS_LINE1') }} AS ADDRESS_LINE1,
{{ transform_string('ADDRESS_LINE2') }} AS ADDRESS_LINE2,
{{ transform_string('ADDRESS_LINE3') }} AS ADDRESS_LINE3,
{{ transform_string('ADDRESS_LINE4') }} AS ADDRESS_LINE4,
{{ transform_string('ADDRESS_CITY') }} AS ADDRESS_CITY,
{{ transform_string('STATE_CODE') }} AS STATE_CODE,
{{ transform_string('CC_ADDRESS') }} AS CC_ADDRESS,
{{ transform_string('ZIP') }} AS ZIP,
{{ transform_string('COUNTRY_CODE') }} AS COUNTRY_CODE,
{{ transform_string('ISSUER_BANK') }} AS ISSUER_BANK,
{{ transform_datetime('CC_START_DATE') }} AS CC_START_DATE,
{{ transform_numeric('CC_ISSUE_NUMBER') }} AS CC_ISSUE_NUMBER,
{{ transform_string('CC_TYPE') }} AS CC_TYPE,
NULL AS CC_ADDTN_CODE,
{{ transform_string('CC_TOKEN') }} AS CC_TOKEN,
NULL AS CC_TOKEN_TYPE,
NULL AS CC_TOKEN_MERCHANT,
NULL AS CC_LAST_NAME,
NULL AS CC_FIRST_NAME,
NULL AS CC_SERVER_TYPE,
NULL AS ALLOW_USE_ONBOARD,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
{{ transform_string('TOKEN_TYPE') }} AS TOKEN_TYPE
    from {{ source('AMA_PROD_BRNZ_SW1', 'CC_CLIENT') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_string('CC_ACC_NUMBER') }} AS CC_ACC_NUMBER,
{{ transform_numeric('CC_ACC_ID') }} AS CC_ACC_ID,
{{ transform_datetime('CC_EXP_DATE') }} AS CC_EXP_DATE,
{{ transform_string('IS_CC_ACTIVE') }} AS IS_CC_ACTIVE,
{{ transform_string('IS_CC_BLACKLISTED') }} AS IS_CC_BLACKLISTED,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_string('CC_NAME_ON_CARD') }} AS CC_NAME_ON_CARD,
{{ transform_string('ADDRESS_LINE1') }} AS ADDRESS_LINE1,
{{ transform_string('ADDRESS_LINE2') }} AS ADDRESS_LINE2,
{{ transform_string('ADDRESS_LINE3') }} AS ADDRESS_LINE3,
{{ transform_string('ADDRESS_LINE4') }} AS ADDRESS_LINE4,
{{ transform_string('ADDRESS_CITY') }} AS ADDRESS_CITY,
{{ transform_string('STATE_CODE') }} AS STATE_CODE,
{{ transform_string('CC_ADDRESS') }} AS CC_ADDRESS,
{{ transform_string('ZIP') }} AS ZIP,
{{ transform_string('COUNTRY_CODE') }} AS COUNTRY_CODE,
{{ transform_string('ISSUER_BANK') }} AS ISSUER_BANK,
{{ transform_datetime('CC_START_DATE') }} AS CC_START_DATE,
{{ transform_numeric('CC_ISSUE_NUMBER') }} AS CC_ISSUE_NUMBER,
{{ transform_string('CC_TYPE') }} AS CC_TYPE,
{{ transform_string('CC_ADDTN_CODE') }} AS CC_ADDTN_CODE,
{{ transform_string('CC_TOKEN') }} AS CC_TOKEN,
{{ transform_string('CC_TOKEN_TYPE') }} AS CC_TOKEN_TYPE,
{{ transform_string('CC_TOKEN_MERCHANT') }} AS CC_TOKEN_MERCHANT,
{{ transform_string('CC_LAST_NAME') }} AS CC_LAST_NAME,
{{ transform_string('CC_FIRST_NAME') }} AS CC_FIRST_NAME,
{{ transform_string('CC_SERVER_TYPE') }} AS CC_SERVER_TYPE,
{{ transform_string('ALLOW_USE_ONBOARD') }} AS ALLOW_USE_ONBOARD,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
NULL AS TOKEN_TYPE
    from {{ source('AMA_PROD_BRNZ_SW2', 'CC_CLIENT') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
