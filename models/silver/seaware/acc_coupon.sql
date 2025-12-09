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
        unique_key=['COUPON_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'ACC_COUPON') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_acc_coupon',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='ACC_COUPON',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'ACC_COUPON') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'ACC_COUPON', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'ACC_COUPON') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'ACC_COUPON', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'ACC_COUPON') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'ACC_COUPON') %}
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
{{ transform_numeric('COUPON_ID') }} AS COUPON_ID,
{{ transform_datetime('VALID_FROM') }} AS VALID_FROM,
{{ transform_datetime('VALID_TO') }} AS VALID_TO,
{{ transform_numeric('AMOUNT') }} AS AMOUNT,
{{ transform_numeric('AMOUNT_LEFT') }} AS AMOUNT_LEFT,
{{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_string('COUPON_CLASS') }} AS COUPON_CLASS,
{{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('GEN_LINK_ID') }} AS GEN_LINK_ID,
{{ transform_string('ENTITY_TYPE') }} AS ENTITY_TYPE,
{{ transform_numeric('ENTITY_ID') }} AS ENTITY_ID,
{{ transform_string('IS_USED') }} AS IS_USED,
{{ transform_numeric('TEMP_RES_ID') }} AS TEMP_RES_ID,
{{ transform_string('PAP_CODE') }} AS PAP_CODE,
{{ transform_numeric('GRANTED_TO_CLIENT_ID') }} AS GRANTED_TO_CLIENT_ID,
{{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
{{ transform_numeric('FREQUENT_PGM_POINTS') }} AS FREQUENT_PGM_POINTS,
{{ transform_string('REASON_CODE') }} AS REASON_CODE,
{{ transform_datetime('EFFECTIVE_FROM') }} AS EFFECTIVE_FROM,
{{ transform_datetime('EFFECTIVE_TO') }} AS EFFECTIVE_TO,
NULL AS RES_ADDON_ID,
NULL AS DAYS_AFTER_FIRST_USE_EFF,
NULL AS NO_SHOW_DEP_REF_ID,
NULL AS GL_EXPIRATION_ERROR,
NULL AS GL_CREATION_DATE,
NULL AS GL_CREATION_ERROR,
NULL AS LINKED_TRANS_ID,
NULL AS USER_DEFINED_COUPON_ID,
NULL AS COUPON_CATEGORY,
NULL AS CHARGE_CODE,
NULL AS DATE_USED,
NULL AS DATE_CREATED,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
NULL AS GL_EXPIRATION_DATE
    from {{ source('AMA_PROD_BRNZ_SW1', 'ACC_COUPON') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('COUPON_ID') }} AS COUPON_ID,
{{ transform_datetime('VALID_FROM') }} AS VALID_FROM,
{{ transform_datetime('VALID_TO') }} AS VALID_TO,
{{ transform_numeric('AMOUNT') }} AS AMOUNT,
{{ transform_numeric('AMOUNT_LEFT') }} AS AMOUNT_LEFT,
{{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_string('COUPON_CLASS') }} AS COUPON_CLASS,
{{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('GEN_LINK_ID') }} AS GEN_LINK_ID,
{{ transform_string('ENTITY_TYPE') }} AS ENTITY_TYPE,
{{ transform_numeric('ENTITY_ID') }} AS ENTITY_ID,
{{ transform_string('IS_USED') }} AS IS_USED,
{{ transform_numeric('TEMP_RES_ID') }} AS TEMP_RES_ID,
{{ transform_string('PAP_CODE') }} AS PAP_CODE,
{{ transform_numeric('GRANTED_TO_CLIENT_ID') }} AS GRANTED_TO_CLIENT_ID,
{{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
{{ transform_numeric('FREQUENT_PGM_POINTS') }} AS FREQUENT_PGM_POINTS,
{{ transform_string('REASON_CODE') }} AS REASON_CODE,
{{ transform_datetime('EFFECTIVE_FROM') }} AS EFFECTIVE_FROM,
{{ transform_datetime('EFFECTIVE_TO') }} AS EFFECTIVE_TO,
{{ transform_numeric('RES_ADDON_ID') }} AS RES_ADDON_ID,
{{ transform_numeric('DAYS_AFTER_FIRST_USE_EFF') }} AS DAYS_AFTER_FIRST_USE_EFF,
{{ transform_numeric('NO_SHOW_DEP_REF_ID') }} AS NO_SHOW_DEP_REF_ID,
{{ transform_string('GL_EXPIRATION_ERROR') }} AS GL_EXPIRATION_ERROR,
{{ transform_datetime('GL_CREATION_DATE') }} AS GL_CREATION_DATE,
{{ transform_string('GL_CREATION_ERROR') }} AS GL_CREATION_ERROR,
{{ transform_numeric('LINKED_TRANS_ID') }} AS LINKED_TRANS_ID,
{{ transform_string('USER_DEFINED_COUPON_ID') }} AS USER_DEFINED_COUPON_ID,
{{ transform_string('COUPON_CATEGORY') }} AS COUPON_CATEGORY,
{{ transform_string('CHARGE_CODE') }} AS CHARGE_CODE,
{{ transform_datetime('DATE_USED') }} AS DATE_USED,
{{ transform_datetime('DATE_CREATED') }} AS DATE_CREATED,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
{{ transform_datetime('GL_EXPIRATION_DATE') }} AS GL_EXPIRATION_DATE
    from {{ source('AMA_PROD_BRNZ_SW2', 'ACC_COUPON') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
