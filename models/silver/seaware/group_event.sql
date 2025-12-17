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
        unique_key=['RECORD_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'GROUP_EVENT') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'GROUP_EVENT') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'GROUP_EVENT', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'GROUP_EVENT') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'GROUP_EVENT', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'GROUP_EVENT') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'GROUP_EVENT') %}
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
            {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
            {{ transform_string('GROUP_EVENT_TYPE') }} AS GROUP_EVENT_TYPE,
            {{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
            {{ transform_datetime('EVENT_TIMESTAMP') }} AS EVENT_TIMESTAMP,
            {{ transform_date('EVENT_DATE') }} AS EVENT_DATE,
            {{ transform_string('OPERATOR_ID') }} AS OPERATOR_ID,
            {{ transform_string('OLD_STATUS') }} AS OLD_STATUS,
            {{ transform_string('NEW_STATUS') }} AS NEW_STATUS,
            {{ transform_string('OLD_SHIP_CODE') }} AS OLD_SHIP_CODE,
            {{ transform_date('OLD_SAIL_FROM') }} AS OLD_SAIL_FROM,
            {{ transform_date('OLD_SAIL_TO') }} AS OLD_SAIL_TO,
            {{ transform_string('NEW_SHIP_CODE') }} AS NEW_SHIP_CODE,
            {{ transform_date('NEW_SAIL_FROM') }} AS NEW_SAIL_FROM,
            {{ transform_date('NEW_SAIL_TO') }} AS NEW_SAIL_TO,
            {{ transform_numeric('OLD_CABINS') }} AS OLD_CABINS,
            {{ transform_numeric('OLD_GUEST_COUNT') }} AS OLD_GUEST_COUNT,
            {{ transform_numeric('NEW_CABINS') }} AS NEW_CABINS,
            {{ transform_numeric('NEW_GUEST_COUNT') }} AS NEW_GUEST_COUNT,
            {{ transform_numeric('OLD_PROBABILITY') }} AS OLD_PROBABILITY,
            {{ transform_numeric('NEW_PROBABILITY') }} AS NEW_PROBABILITY,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'GROUP_EVENT') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
            {{ transform_string('GROUP_EVENT_TYPE') }} AS GROUP_EVENT_TYPE,
            {{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
            {{ transform_datetime('EVENT_TIMESTAMP') }} AS EVENT_TIMESTAMP,
            {{ transform_date('EVENT_DATE') }} AS EVENT_DATE,
            {{ transform_string('OPERATOR_ID') }} AS OPERATOR_ID,
            {{ transform_string('OLD_STATUS') }} AS OLD_STATUS,
            {{ transform_string('NEW_STATUS') }} AS NEW_STATUS,
            {{ transform_string('OLD_SHIP_CODE') }} AS OLD_SHIP_CODE,
            {{ transform_datetime('OLD_SAIL_FROM') }} AS OLD_SAIL_FROM,
            {{ transform_datetime('OLD_SAIL_TO') }} AS OLD_SAIL_TO,
            {{ transform_string('NEW_SHIP_CODE') }} AS NEW_SHIP_CODE,
            {{ transform_datetime('NEW_SAIL_FROM') }} AS NEW_SAIL_FROM,
            {{ transform_datetime('NEW_SAIL_TO') }} AS NEW_SAIL_TO,
            {{ transform_numeric('OLD_CABINS') }} AS OLD_CABINS,
            {{ transform_numeric('OLD_GUEST_COUNT') }} AS OLD_GUEST_COUNT,
            {{ transform_numeric('NEW_CABINS') }} AS NEW_CABINS,
            {{ transform_numeric('NEW_GUEST_COUNT') }} AS NEW_GUEST_COUNT,
            {{ transform_numeric('OLD_PROBABILITY') }} AS OLD_PROBABILITY,
            {{ transform_numeric('NEW_PROBABILITY') }} AS NEW_PROBABILITY,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'GROUP_EVENT') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["RECORD_ID", "DATA_SOURCE"]) }} AS GROUP_EVENT_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["RECORD_ID", "DATA_SOURCE"]) }} AS GROUP_EVENT_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

