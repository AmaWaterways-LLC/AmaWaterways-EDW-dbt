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
        unique_key=['PACKAGE_TYPE', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'PACKAGE_TYPE') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'PACKAGE_TYPE') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'PACKAGE_TYPE', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'PACKAGE_TYPE') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'PACKAGE_TYPE', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'PACKAGE_TYPE') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'PACKAGE_TYPE') %}
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
            {{ transform_numeric('PACKAGE_TYPE_ID') }} AS PACKAGE_TYPE_ID,
            {{ transform_string('PACKAGE_TYPE') }} AS PACKAGE_TYPE,
            {{ transform_numeric('LAND_DAYS') }} AS LAND_DAYS,
            {{ transform_numeric('SAIL_DAYS') }} AS SAIL_DAYS,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('PRODUCT_TYPE') }} AS PRODUCT_TYPE,
            {{ transform_string('TC_PACKAGE_TYPE') }} AS TC_PACKAGE_TYPE,
            {{ transform_numeric('LAND_DAYS_POST') }} AS LAND_DAYS_POST,
            {{ transform_string('IS_SHOREX') }} AS IS_SHOREX,
            {{ transform_string('IS_SECONDARY') }} AS IS_SECONDARY,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_string('SHOREX_TIMING') }} AS SHOREX_TIMING,
            {{ transform_string('EXTRA_SEAT_QUESTION') }} AS EXTRA_SEAT_QUESTION,
            {{ transform_string('PRE_POST_MODE') }} AS PRE_POST_MODE,
            NULL AS PACKAGE_CLASS,
            NULL AS SAIL_SEGMENTS,
            NULL AS CAPACITY,
            NULL AS ALLOW_SEGMENTS,
            NULL AS PACKAGE_TYPE_NAME,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_string('IS_LAND_ONLY') }} AS IS_LAND_ONLY,
            {{ transform_string('IS_MULTI_SAIL') }} AS IS_MULTI_SAIL,
            {{ transform_string('AIR_BY_ESEAAIR') }} AS AIR_BY_ESEAAIR
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'PACKAGE_TYPE') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('PACKAGE_TYPE_ID') }} AS PACKAGE_TYPE_ID,
            {{ transform_string('PACKAGE_TYPE') }} AS PACKAGE_TYPE,
            {{ transform_numeric('LAND_DAYS') }} AS LAND_DAYS,
            {{ transform_numeric('SAIL_DAYS') }} AS SAIL_DAYS,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('PRODUCT_TYPE') }} AS PRODUCT_TYPE,
            {{ transform_string('TC_PACKAGE_TYPE') }} AS TC_PACKAGE_TYPE,
            {{ transform_numeric('LAND_DAYS_POST') }} AS LAND_DAYS_POST,
            {{ transform_string('IS_SHOREX') }} AS IS_SHOREX,
            {{ transform_string('IS_SECONDARY') }} AS IS_SECONDARY,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_string('SHOREX_TIMING') }} AS SHOREX_TIMING,
            {{ transform_string('EXTRA_SEAT_QUESTION') }} AS EXTRA_SEAT_QUESTION,
            {{ transform_string('PRE_POST_MODE') }} AS PRE_POST_MODE,
            {{ transform_string('PACKAGE_CLASS') }} AS PACKAGE_CLASS,
            {{ transform_string('SAIL_SEGMENTS') }} AS SAIL_SEGMENTS,
            {{ transform_numeric('CAPACITY') }} AS CAPACITY,
            {{ transform_string('ALLOW_SEGMENTS') }} AS ALLOW_SEGMENTS,
            {{ transform_string('PACKAGE_TYPE_NAME') }} AS PACKAGE_TYPE_NAME,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            NULL AS IS_LAND_ONLY,
            NULL AS IS_MULTI_SAIL,
            NULL AS AIR_BY_ESEAAIR
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'PACKAGE_TYPE') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["PACKAGE_TYPE", "DATA_SOURCE"]) }} AS PACKAGE_TYPE_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["PACKAGE_TYPE", "DATA_SOURCE"]) }} AS PACKAGE_TYPE_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

