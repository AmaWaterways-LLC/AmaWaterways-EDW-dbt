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
        unique_key=['RTG_SEGMENT_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'RES_AIR_REQUEST_SEGMENT') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'RES_AIR_REQUEST_SEGMENT') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'RES_AIR_REQUEST_SEGMENT', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'RES_AIR_REQUEST_SEGMENT') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'RES_AIR_REQUEST_SEGMENT', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'RES_AIR_REQUEST_SEGMENT') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'RES_AIR_REQUEST_SEGMENT') %}
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

WITH src AS (
    SELECT
            'SW1' AS DATA_SOURCE,
            {{ transform_numeric('RTG_SEGMENT_ID') }} AS RTG_SEGMENT_ID,
            {{ transform_numeric('AIR_REQUEST_ID') }} AS AIR_REQUEST_ID,
            {{ transform_numeric('SEGMENT_SEQN') }} AS SEGMENT_SEQN,
            {{ transform_string('DIRECTION') }} AS DIRECTION,
            {{ transform_string('CARRIER_CODE') }} AS CARRIER_CODE,
            {{ transform_string('FLIGHT') }} AS FLIGHT,
            {{ transform_string('AIRPORT_FROM') }} AS AIRPORT_FROM,
            {{ transform_string('AIRPORT_TO') }} AS AIRPORT_TO,
            {{ transform_date('DEPARTURE_DATE') }} AS DEPARTURE_DATE,
            {{ transform_datetime('DEPARTURE_TIME') }} AS DEPARTURE_TIME,
            {{ transform_datetime('DEPARTURE_BEST_TIME') }} AS DEPARTURE_BEST_TIME,
            {{ transform_datetime('DEPARTURE_EARLIEST_TIME') }} AS DEPARTURE_EARLIEST_TIME,
            {{ transform_datetime('DEPARTURE_LATEST_TIME') }} AS DEPARTURE_LATEST_TIME,
            {{ transform_date('ARRIVAL_DATE') }} AS ARRIVAL_DATE,
            {{ transform_datetime('ARRIVAL_TIME') }} AS ARRIVAL_TIME,
            {{ transform_datetime('ARRIVAL_BEST_TIME') }} AS ARRIVAL_BEST_TIME,
            {{ transform_datetime('ARRIVAL_EARLIEST_TIME') }} AS ARRIVAL_EARLIEST_TIME,
            {{ transform_datetime('ARRIVAL_LATEST_TIME') }} AS ARRIVAL_LATEST_TIME,
            {{ transform_string('CHARTER_FLIGHT') }} AS CHARTER_FLIGHT,
            {{ transform_numeric('QUEUED_REQUEST_ID') }} AS QUEUED_REQUEST_ID,
            {{ transform_string('TIMING') }} AS TIMING,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'RES_AIR_REQUEST_SEGMENT') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full_sw1 %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["RTG_SEGMENT_ID", "DATA_SOURCE"]) }} AS RES_AIR_REQUEST_SEGMENT_SURROGATE_KEY,
    src.*
FROM src

