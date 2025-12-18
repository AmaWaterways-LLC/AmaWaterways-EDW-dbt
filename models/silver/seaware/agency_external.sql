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
        unique_key=['AGENCY_EXTERNAL_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'AGENCY_EXTERNAL') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'AGENCY_EXTERNAL') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'AGENCY_EXTERNAL', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'AGENCY_EXTERNAL') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'AGENCY_EXTERNAL', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'AGENCY_EXTERNAL') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'AGENCY_EXTERNAL') %}
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
            {{ transform_numeric('AGENCY_EXTERNAL_ID') }} AS AGENCY_EXTERNAL_ID,
            {{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
            {{ transform_numeric('EXTERNAL_NAMES_ID') }} AS EXTERNAL_NAMES_ID,
            {{ transform_string('PSEUDO_CITY_CODE') }} AS PSEUDO_CITY_CODE,
            {{ transform_string('ACTIVE') }} AS ACTIVE,
            {{ transform_string('HAS_ACCESSED') }} AS HAS_ACCESSED,
            {{ transform_date('FIRST_TIME_ACCESSED') }} AS FIRST_TIME_ACCESSED,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source(var('bronze_source_prefix') ~ '_SW1', 'AGENCY_EXTERNAL') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('AGENCY_EXTERNAL_ID') }} AS AGENCY_EXTERNAL_ID,
            {{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
            {{ transform_numeric('EXTERNAL_NAMES_ID') }} AS EXTERNAL_NAMES_ID,
            {{ transform_string('PSEUDO_CITY_CODE') }} AS PSEUDO_CITY_CODE,
            {{ transform_string('ACTIVE') }} AS ACTIVE,
            {{ transform_string('HAS_ACCESSED') }} AS HAS_ACCESSED,
            {{ transform_date('FIRST_TIME_ACCESSED') }} AS FIRST_TIME_ACCESSED,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source(var('bronze_source_prefix') ~ '_SW2', 'AGENCY_EXTERNAL') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT *
FROM (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["AGENCY_EXTERNAL_ID", "DATA_SOURCE"]) }} AS AGENCY_EXTERNAL_SURROGATE_KEY,
        sw1_src.*
    FROM sw1_src

    UNION ALL

    SELECT
        {{ dbt_utils.generate_surrogate_key(["AGENCY_EXTERNAL_ID", "DATA_SOURCE"]) }} AS AGENCY_EXTERNAL_SURROGATE_KEY,
        sw2_src.*
    FROM sw2_src
)
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY AGENCY_EXTERNAL_ID, DATA_SOURCE
        ORDER BY LAST_UPDATED_TIMESTAMP DESC
) = 1

