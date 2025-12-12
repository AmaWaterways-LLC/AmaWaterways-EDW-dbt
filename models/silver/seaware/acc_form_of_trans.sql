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
        unique_key=['FORM_OF_TRANS', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'ACC_FORM_OF_TRANS') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'ACC_FORM_OF_TRANS') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'ACC_FORM_OF_TRANS', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'ACC_FORM_OF_TRANS') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'ACC_FORM_OF_TRANS', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'ACC_FORM_OF_TRANS') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'ACC_FORM_OF_TRANS') %}
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
            {{ transform_string('FORM_OF_TRANS') }} AS FORM_OF_TRANS,
            {{ transform_numeric('FORM_OF_TRANS_ID') }} AS FORM_OF_TRANS_ID,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('CLASS_OF_TRANS') }} AS CLASS_OF_TRANS,
            {{ transform_string('FORM_OF_TRANS_NAME') }} AS FORM_OF_TRANS_NAME,
            {{ transform_string('IS_DEFAULT_FOR_CLASS') }} AS IS_DEFAULT_FOR_CLASS,
            {{ transform_string('PROGRAM_NAME') }} AS PROGRAM_NAME,
            {{ transform_string('PARAMS') }} AS PARAMS,
            NULL AS SEQN,
            NULL AS USE_REFUND_FORM,
            NULL AS BALANCE_AT_CANCEL,
            NULL AS CLIENT_ID_IS_MANDATORY,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'ACC_FORM_OF_TRANS') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_string('FORM_OF_TRANS') }} AS FORM_OF_TRANS,
            {{ transform_numeric('FORM_OF_TRANS_ID') }} AS FORM_OF_TRANS_ID,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('CLASS_OF_TRANS') }} AS CLASS_OF_TRANS,
            {{ transform_string('FORM_OF_TRANS_NAME') }} AS FORM_OF_TRANS_NAME,
            {{ transform_string('IS_DEFAULT_FOR_CLASS') }} AS IS_DEFAULT_FOR_CLASS,
            {{ transform_string('PROGRAM_NAME') }} AS PROGRAM_NAME,
            {{ transform_string('PARAMS') }} AS PARAMS,
            {{ transform_numeric('SEQN') }} AS SEQN,
            {{ transform_string('USE_REFUND_FORM') }} AS USE_REFUND_FORM,
            {{ transform_string('BALANCE_AT_CANCEL') }} AS BALANCE_AT_CANCEL,
            {{ transform_string('CLIENT_ID_IS_MANDATORY') }} AS CLIENT_ID_IS_MANDATORY,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'ACC_FORM_OF_TRANS') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["FORM_OF_TRANS", "DATA_SOURCE"]) }} AS ACC_FORM_OF_TRANS_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["FORM_OF_TRANS", "DATA_SOURCE"]) }} AS ACC_FORM_OF_TRANS_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

