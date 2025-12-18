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
        unique_key=['PACKAGE_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'PACKAGE_DEFINITION') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'PACKAGE_DEFINITION') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'PACKAGE_DEFINITION', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'PACKAGE_DEFINITION') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'PACKAGE_DEFINITION', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'PACKAGE_DEFINITION') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'PACKAGE_DEFINITION') %}
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
            {{ transform_numeric('PACKAGE_ID') }} AS PACKAGE_ID,
            {{ transform_string('PACKAGE_CODE') }} AS PACKAGE_CODE,
            {{ transform_datetime('VACATION_DATE') }} AS VACATION_DATE,
            {{ transform_string('PACKAGE_TYPE') }} AS PACKAGE_TYPE,
            {{ transform_numeric('SAIL_ID') }} AS SAIL_ID,
            {{ transform_string('SEASON_CODE') }} AS SEASON_CODE,
            {{ transform_string('GEOG_AREA_CODE') }} AS GEOG_AREA_CODE,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_numeric('SORT_SEQN') }} AS SORT_SEQN,
            {{ transform_string('SHOREX_MODE') }} AS SHOREX_MODE,
            {{ transform_string('SHOREX_TIMING') }} AS SHOREX_TIMING,
            NULL AS INITIAL_STATUS,
            NULL AS ROUTE_CODE,
            NULL AS PACKAGE_NAME,
            NULL AS SAIL_SEGMENTS,
            NULL AS SAIL_DAYS_OVERRIDE,
            NULL AS POINTS_SAIL_SEGMENTS,
            NULL AS TOUR_STATUS,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source(var('bronze_source_prefix') ~ '_SW1', 'PACKAGE_DEFINITION') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('PACKAGE_ID') }} AS PACKAGE_ID,
            {{ transform_string('PACKAGE_CODE') }} AS PACKAGE_CODE,
            {{ transform_datetime('VACATION_DATE') }} AS VACATION_DATE,
            {{ transform_string('PACKAGE_TYPE') }} AS PACKAGE_TYPE,
            {{ transform_numeric('SAIL_ID') }} AS SAIL_ID,
            {{ transform_string('SEASON_CODE') }} AS SEASON_CODE,
            {{ transform_string('GEOG_AREA_CODE') }} AS GEOG_AREA_CODE,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_numeric('SORT_SEQN') }} AS SORT_SEQN,
            {{ transform_string('SHOREX_MODE') }} AS SHOREX_MODE,
            {{ transform_string('SHOREX_TIMING') }} AS SHOREX_TIMING,
            {{ transform_string('INITIAL_STATUS') }} AS INITIAL_STATUS,
            {{ transform_string('ROUTE_CODE') }} AS ROUTE_CODE,
            {{ transform_string('PACKAGE_NAME') }} AS PACKAGE_NAME,
            {{ transform_string('SAIL_SEGMENTS') }} AS SAIL_SEGMENTS,
            {{ transform_numeric('SAIL_DAYS_OVERRIDE') }} AS SAIL_DAYS_OVERRIDE,
            {{ transform_string('POINTS_SAIL_SEGMENTS') }} AS POINTS_SAIL_SEGMENTS,
            {{ transform_string('TOUR_STATUS') }} AS TOUR_STATUS,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source(var('bronze_source_prefix') ~ '_SW2', 'PACKAGE_DEFINITION') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT *
FROM (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["PACKAGE_ID", "DATA_SOURCE"]) }} AS PACKAGE_DEFINITION_SURROGATE_KEY,
        sw1_src.*
    FROM sw1_src

    UNION ALL

    SELECT
        {{ dbt_utils.generate_surrogate_key(["PACKAGE_ID", "DATA_SOURCE"]) }} AS PACKAGE_DEFINITION_SURROGATE_KEY,
        sw2_src.*
    FROM sw2_src
)
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY PACKAGE_ID, DATA_SOURCE
        ORDER BY LAST_UPDATED_TIMESTAMP DESC
) = 1

