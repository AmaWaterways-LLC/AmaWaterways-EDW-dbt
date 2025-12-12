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
        unique_key=['SAIL_ACTIVITY_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'SAIL_ACTIVITY') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'SAIL_ACTIVITY') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'SAIL_ACTIVITY', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'SAIL_ACTIVITY') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'SAIL_ACTIVITY', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'SAIL_ACTIVITY') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'SAIL_ACTIVITY') %}
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
            {{ transform_numeric('SAIL_ACTIVITY_ID') }} AS SAIL_ACTIVITY_ID,
            {{ transform_datetime('SAIL_EVENT_DATE_TIME') }} AS SAIL_EVENT_DATE_TIME,
            {{ transform_string('SAIL_EVENT_TYPE') }} AS SAIL_EVENT_TYPE,
            {{ transform_string('PORT_CODE') }} AS PORT_CODE,
            {{ transform_string('PIER_CODE') }} AS PIER_CODE,
            {{ transform_string('MAY_EMBARK') }} AS MAY_EMBARK,
            {{ transform_string('MAY_DISEMBARK') }} AS MAY_DISEMBARK,
            {{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
            {{ transform_numeric('RELATIVE_DAY') }} AS RELATIVE_DAY,
            {{ transform_string('ACTIVITY') }} AS ACTIVITY,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            NULL AS DEP_ARR_CODE,
            NULL AS DEP_ARR_REF_ID,
            NULL AS NOTES,
            NULL AS OWNERSHIP_CODE,
            NULL AS SETUP_SESSION_ID,
            NULL AS DEL_SESSION_ID,
            NULL AS SETUP_FROM,
            NULL AS SETUP_TO,
            NULL AS TENDER_MODE,
            NULL AS SAIL_EVENT_TS,
            NULL AS DRESS_CODE,
            NULL AS IS_ACTIVE,
            NULL AS SAIL_EVENT_DATE_TIME_UTC,
            NULL AS DECK_ONLY,
            NULL AS ACTIVITY_CODE,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'SAIL_ACTIVITY') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('SAIL_ACTIVITY_ID') }} AS SAIL_ACTIVITY_ID,
            {{ transform_datetime('SAIL_EVENT_DATE_TIME') }} AS SAIL_EVENT_DATE_TIME,
            {{ transform_string('SAIL_EVENT_TYPE') }} AS SAIL_EVENT_TYPE,
            {{ transform_string('PORT_CODE') }} AS PORT_CODE,
            {{ transform_string('PIER_CODE') }} AS PIER_CODE,
            {{ transform_string('MAY_EMBARK') }} AS MAY_EMBARK,
            {{ transform_string('MAY_DISEMBARK') }} AS MAY_DISEMBARK,
            {{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
            {{ transform_numeric('RELATIVE_DAY') }} AS RELATIVE_DAY,
            {{ transform_string('ACTIVITY') }} AS ACTIVITY,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('DEP_ARR_CODE') }} AS DEP_ARR_CODE,
            {{ transform_numeric('DEP_ARR_REF_ID') }} AS DEP_ARR_REF_ID,
            {{ transform_string('NOTES') }} AS NOTES,
            {{ transform_string('OWNERSHIP_CODE') }} AS OWNERSHIP_CODE,
            {{ transform_numeric('SETUP_SESSION_ID') }} AS SETUP_SESSION_ID,
            {{ transform_numeric('DEL_SESSION_ID') }} AS DEL_SESSION_ID,
            {{ transform_datetime('SETUP_FROM') }} AS SETUP_FROM,
            {{ transform_datetime('SETUP_TO') }} AS SETUP_TO,
            {{ transform_string('TENDER_MODE') }} AS TENDER_MODE,
            {{ transform_datetime('SAIL_EVENT_TS') }} AS SAIL_EVENT_TS,
            {{ transform_string('DRESS_CODE') }} AS DRESS_CODE,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_datetime('SAIL_EVENT_DATE_TIME_UTC') }} AS SAIL_EVENT_DATE_TIME_UTC,
            {{ transform_string('DECK_ONLY') }} AS DECK_ONLY,
            {{ transform_string('ACTIVITY_CODE') }} AS ACTIVITY_CODE,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'SAIL_ACTIVITY') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["SAIL_ACTIVITY_ID", "DATA_SOURCE"]) }} AS SAIL_ACTIVITY_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["SAIL_ACTIVITY_ID", "DATA_SOURCE"]) }} AS SAIL_ACTIVITY_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

