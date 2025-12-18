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
        unique_key=['COUPON_CLASS', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'ACC_COUPON_CLASS') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'ACC_COUPON_CLASS') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'ACC_COUPON_CLASS', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'ACC_COUPON_CLASS') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'ACC_COUPON_CLASS', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'ACC_COUPON_CLASS') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'ACC_COUPON_CLASS') %}
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
            {{ transform_string('COUPON_CLASS') }} AS COUPON_CLASS,
            {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('EXTERNAL_CODE') }} AS EXTERNAL_CODE,
            {{ transform_string('IS_GEN_PER_CLIENT') }} AS IS_GEN_PER_CLIENT,
            {{ transform_string('APPLY_AS_PAYMENT') }} AS APPLY_AS_PAYMENT,
            {{ transform_string('IS_APPLY_PER_CLIENT') }} AS IS_APPLY_PER_CLIENT,
            {{ transform_numeric('DFLT_QTY_VALID_DAYS') }} AS DFLT_QTY_VALID_DAYS,
            {{ transform_string('FREQUENT_PGM_CODE') }} AS FREQUENT_PGM_CODE,
            {{ transform_string('CANCEL_WITH_RES') }} AS CANCEL_WITH_RES,
            NULL AS APPLY_AS_DISCOUNT,
            NULL AS VALIDATE_FOR_OWNER,
            NULL AS PRE_PRINTED,
            NULL AS COUPON_CLASS_CODE,
            NULL AS NO_APPL_RULES,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_string('GENERATE_ADDON') }} AS GENERATE_ADDON,
            {{ transform_string('IS_VOUCHER') }} AS IS_VOUCHER
    FROM {{ source(var('bronze_source_prefix') ~ '_SW1', 'ACC_COUPON_CLASS') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_string('COUPON_CLASS') }} AS COUPON_CLASS,
            {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('EXTERNAL_CODE') }} AS EXTERNAL_CODE,
            {{ transform_string('IS_GEN_PER_CLIENT') }} AS IS_GEN_PER_CLIENT,
            {{ transform_string('APPLY_AS_PAYMENT') }} AS APPLY_AS_PAYMENT,
            {{ transform_string('IS_APPLY_PER_CLIENT') }} AS IS_APPLY_PER_CLIENT,
            {{ transform_numeric('DFLT_QTY_VALID_DAYS') }} AS DFLT_QTY_VALID_DAYS,
            {{ transform_string('FREQUENT_PGM_CODE') }} AS FREQUENT_PGM_CODE,
            {{ transform_string('CANCEL_WITH_RES') }} AS CANCEL_WITH_RES,
            {{ transform_string('APPLY_AS_DISCOUNT') }} AS APPLY_AS_DISCOUNT,
            {{ transform_string('VALIDATE_FOR_OWNER') }} AS VALIDATE_FOR_OWNER,
            {{ transform_string('PRE_PRINTED') }} AS PRE_PRINTED,
            {{ transform_string('COUPON_CLASS_CODE') }} AS COUPON_CLASS_CODE,
            {{ transform_string('NO_APPL_RULES') }} AS NO_APPL_RULES,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            NULL AS GENERATE_ADDON,
            NULL AS IS_VOUCHER
    FROM {{ source(var('bronze_source_prefix') ~ '_SW2', 'ACC_COUPON_CLASS') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT *
FROM (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["COUPON_CLASS", "DATA_SOURCE"]) }} AS ACC_COUPON_CLASS_SURROGATE_KEY,
        sw1_src.*
    FROM sw1_src

    UNION ALL

    SELECT
        {{ dbt_utils.generate_surrogate_key(["COUPON_CLASS", "DATA_SOURCE"]) }} AS ACC_COUPON_CLASS_SURROGATE_KEY,
        sw2_src.*
    FROM sw2_src
)
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY COUPON_CLASS, DATA_SOURCE
        ORDER BY LAST_UPDATED_TIMESTAMP DESC
) = 1

