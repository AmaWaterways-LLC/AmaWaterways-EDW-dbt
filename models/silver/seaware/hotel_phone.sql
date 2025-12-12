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
        unique_key=['HOTEL_ID', 'SEQ_NUMBER', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'HOTEL_PHONE') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'HOTEL_PHONE') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'HOTEL_PHONE', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'HOTEL_PHONE') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'HOTEL_PHONE', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'HOTEL_PHONE') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'HOTEL_PHONE') %}
    {% set wm_col_sw2 = cfg_sw2['WATERMARK_COLUMN'] %}
    {% set last_wm_sw2 = cfg_sw2['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw2 = (last_wm_sw2 is none) %}
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

WITH sw1_src AS (
    SELECT
            'SW1' AS DATA_SOURCE,
            {{ transform_numeric('SEQ_NUMBER') }} AS SEQ_NUMBER,
            {{ transform_string('PHONE_TYPE') }} AS PHONE_TYPE,
            {{ transform_numeric('INTL_CODE') }} AS INTL_CODE,
            {{ transform_string('PHONE_NUMBER') }} AS PHONE_NUMBER,
            {{ transform_string('IS_PHONE_PRIMARY') }} AS IS_PHONE_PRIMARY,
            {{ transform_string('IS_PHONE_SECONDARY') }} AS IS_PHONE_SECONDARY,
            {{ transform_string('IS_PHONE_FAX') }} AS IS_PHONE_FAX,
            {{ transform_string('FAX_ATTENTION_LINE') }} AS FAX_ATTENTION_LINE,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_numeric('HOTEL_ID') }} AS HOTEL_ID,
            {{ transform_numeric('HOTEL_PHONE_ID') }} AS HOTEL_PHONE_ID,
            {{ transform_string('PHONE_EXT') }} AS PHONE_EXT,
            NULL AS COUNTRY_CODE,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'HOTEL_PHONE') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('SEQ_NUMBER') }} AS SEQ_NUMBER,
            {{ transform_string('PHONE_TYPE') }} AS PHONE_TYPE,
            {{ transform_numeric('INTL_CODE') }} AS INTL_CODE,
            {{ transform_string('PHONE_NUMBER') }} AS PHONE_NUMBER,
            {{ transform_string('IS_PHONE_PRIMARY') }} AS IS_PHONE_PRIMARY,
            {{ transform_string('IS_PHONE_SECONDARY') }} AS IS_PHONE_SECONDARY,
            {{ transform_string('IS_PHONE_FAX') }} AS IS_PHONE_FAX,
            {{ transform_string('FAX_ATTENTION_LINE') }} AS FAX_ATTENTION_LINE,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_numeric('HOTEL_ID') }} AS HOTEL_ID,
            {{ transform_numeric('HOTEL_PHONE_ID') }} AS HOTEL_PHONE_ID,
            {{ transform_string('PHONE_EXT') }} AS PHONE_EXT,
            {{ transform_string('COUNTRY_CODE') }} AS COUNTRY_CODE,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'HOTEL_PHONE') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["HOTEL_ID", "SEQ_NUMBER", "DATA_SOURCE"]) }} AS HOTEL_PHONE_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["HOTEL_ID", "SEQ_NUMBER", "DATA_SOURCE"]) }} AS HOTEL_PHONE_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

