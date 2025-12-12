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
        unique_key=['SAIL_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'SAIL_HEADER') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'SAIL_HEADER') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'SAIL_HEADER', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'SAIL_HEADER') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'SAIL_HEADER', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'SAIL_HEADER') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'SAIL_HEADER') %}
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
            {{ transform_numeric('SAIL_ID') }} AS SAIL_ID,
            {{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
            {{ transform_date('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
            {{ transform_date('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
            {{ transform_numeric('REL_DAY_FROM') }} AS REL_DAY_FROM,
            {{ transform_numeric('REL_DAY_TO') }} AS REL_DAY_TO,
            {{ transform_string('SEASON_CODE') }} AS SEASON_CODE,
            {{ transform_string('PORT_FROM') }} AS PORT_FROM,
            {{ transform_string('PORT_TO') }} AS PORT_TO,
            {{ transform_string('GEOG_AREA_CODE') }} AS GEOG_AREA_CODE,
            {{ transform_string('IS_FAKE') }} AS IS_FAKE,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('SAIL_STATUS') }} AS SAIL_STATUS,
            {{ transform_string('SAIL_CODE') }} AS SAIL_CODE,
            NULL AS DEP_REF_ID,
            NULL AS ARR_REF_ID,
            NULL AS ROUTE_CODE,
            {{ transform_string('IS_LOCKED') }} AS IS_LOCKED,
            NULL AS INV_MODE_RESERVED_ONLY,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_date('TC_PREPROC_AT') }} AS TC_PREPROC_AT
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'SAIL_HEADER') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('SAIL_ID') }} AS SAIL_ID,
            {{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
            {{ transform_date('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
            {{ transform_date('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
            {{ transform_numeric('REL_DAY_FROM') }} AS REL_DAY_FROM,
            {{ transform_numeric('REL_DAY_TO') }} AS REL_DAY_TO,
            {{ transform_string('SEASON_CODE') }} AS SEASON_CODE,
            {{ transform_string('PORT_FROM') }} AS PORT_FROM,
            {{ transform_string('PORT_TO') }} AS PORT_TO,
            {{ transform_string('GEOG_AREA_CODE') }} AS GEOG_AREA_CODE,
            {{ transform_string('IS_FAKE') }} AS IS_FAKE,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('SAIL_STATUS') }} AS SAIL_STATUS,
            {{ transform_string('SAIL_CODE') }} AS SAIL_CODE,
            {{ transform_numeric('DEP_REF_ID') }} AS DEP_REF_ID,
            {{ transform_numeric('ARR_REF_ID') }} AS ARR_REF_ID,
            {{ transform_string('ROUTE_CODE') }} AS ROUTE_CODE,
            {{ transform_string('IS_LOCKED') }} AS IS_LOCKED,
            {{ transform_string('INV_MODE_RESERVED_ONLY') }} AS INV_MODE_RESERVED_ONLY,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            NULL AS TC_PREPROC_AT
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'SAIL_HEADER') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["SAIL_ID", "DATA_SOURCE"]) }} AS SAIL_HEADER_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["SAIL_ID", "DATA_SOURCE"]) }} AS SAIL_HEADER_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

