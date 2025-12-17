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
        unique_key=['BATCH_HDR_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'ACC_BATCH_HEADER') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'ACC_BATCH_HEADER') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'ACC_BATCH_HEADER', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'ACC_BATCH_HEADER') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'ACC_BATCH_HEADER', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'ACC_BATCH_HEADER') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'ACC_BATCH_HEADER') %}
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
            {{ transform_numeric('BATCH_HDR_ID') }} AS BATCH_HDR_ID,
            {{ transform_datetime('BATCH_OPEN_TIME_STAMP') }} AS BATCH_OPEN_TIME_STAMP,
            {{ transform_datetime('BATCH_CLOSED_TIME_STAMP') }} AS BATCH_CLOSED_TIME_STAMP,
            {{ transform_string('FORM_OF_TRANS') }} AS FORM_OF_TRANS,
            {{ transform_numeric('BATCH_AMOUNT') }} AS BATCH_AMOUNT,
            {{ transform_string('USER_ID') }} AS USER_ID,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
            {{ transform_string('SOURCE_CODE') }} AS SOURCE_CODE,
            NULL AS SOURCE_ENTITY_TYPE,
            NULL AS SOURCE_ENTITY_ID,
            NULL AS CC_CLIENT_ID,
            NULL AS CHECK_NUMBER,
            NULL AS BATCH_STATUS,
            NULL AS BANK_ID,
            NULL AS POST_DATE,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_string('IS_BATCH_ACTIVE') }} AS IS_BATCH_ACTIVE
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'ACC_BATCH_HEADER') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('BATCH_HDR_ID') }} AS BATCH_HDR_ID,
            {{ transform_datetime('BATCH_OPEN_TIME_STAMP') }} AS BATCH_OPEN_TIME_STAMP,
            {{ transform_datetime('BATCH_CLOSED_TIME_STAMP') }} AS BATCH_CLOSED_TIME_STAMP,
            {{ transform_string('FORM_OF_TRANS') }} AS FORM_OF_TRANS,
            {{ transform_numeric('BATCH_AMOUNT') }} AS BATCH_AMOUNT,
            {{ transform_string('USER_ID') }} AS USER_ID,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
            {{ transform_string('SOURCE_CODE') }} AS SOURCE_CODE,
            {{ transform_string('SOURCE_ENTITY_TYPE') }} AS SOURCE_ENTITY_TYPE,
            {{ transform_numeric('SOURCE_ENTITY_ID') }} AS SOURCE_ENTITY_ID,
            {{ transform_numeric('CC_CLIENT_ID') }} AS CC_CLIENT_ID,
            {{ transform_string('CHECK_NUMBER') }} AS CHECK_NUMBER,
            {{ transform_string('BATCH_STATUS') }} AS BATCH_STATUS,
            {{ transform_numeric('BANK_ID') }} AS BANK_ID,
            {{ transform_date('POST_DATE') }} AS POST_DATE,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            NULL AS IS_BATCH_ACTIVE
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'ACC_BATCH_HEADER') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["BATCH_HDR_ID", "DATA_SOURCE"]) }} AS ACC_BATCH_HEADER_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["BATCH_HDR_ID", "DATA_SOURCE"]) }} AS ACC_BATCH_HEADER_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

