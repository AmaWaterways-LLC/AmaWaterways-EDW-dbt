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
        unique_key=['RES_COMPONENT_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'RES_COMPONENT') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'RES_COMPONENT') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'RES_COMPONENT', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'RES_COMPONENT') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'RES_COMPONENT', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'RES_COMPONENT') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'RES_COMPONENT') %}
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
            {{ transform_numeric('RES_COMPONENT_ID') }} AS RES_COMPONENT_ID,
            {{ transform_datetime('EFFECTIVE_DATE') }} AS EFFECTIVE_DATE,
            {{ transform_datetime('END_DATE') }} AS END_DATE,
            {{ transform_datetime('START_DATE') }} AS START_DATE,
            {{ transform_string('COMPONENT_CODE') }} AS COMPONENT_CODE,
            {{ transform_numeric('RES_ID') }} AS RES_ID,
            {{ transform_string('COMPONENT_TYPE') }} AS COMPONENT_TYPE,
            {{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
            {{ transform_numeric('RES_PACKAGE_ID') }} AS RES_PACKAGE_ID,
            {{ transform_string('COMPONENT_SUBCODE1') }} AS COMPONENT_SUBCODE1,
            {{ transform_string('COMPONENT_SUBCODE2') }} AS COMPONENT_SUBCODE2,
            {{ transform_string('COMPONENT_SUBCODE3') }} AS COMPONENT_SUBCODE3,
            {{ transform_string('LOCATION_TYPE_FROM') }} AS LOCATION_TYPE_FROM,
            {{ transform_string('LOCATION_CODE_FROM') }} AS LOCATION_CODE_FROM,
            {{ transform_string('LOCATION_TYPE_TO') }} AS LOCATION_TYPE_TO,
            {{ transform_string('LOCATION_CODE_TO') }} AS LOCATION_CODE_TO,
            {{ transform_numeric('ITIN_RECORD_ID') }} AS ITIN_RECORD_ID,
            NULL AS MACRO_PACKAGE_ID,
            NULL AS MACRO_PACKAGE_SEQN,
            NULL AS PARENT_PACKAGE_ID,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'RES_COMPONENT') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('RES_COMPONENT_ID') }} AS RES_COMPONENT_ID,
            {{ transform_datetime('EFFECTIVE_DATE') }} AS EFFECTIVE_DATE,
            {{ transform_datetime('END_DATE') }} AS END_DATE,
            {{ transform_datetime('START_DATE') }} AS START_DATE,
            {{ transform_string('COMPONENT_CODE') }} AS COMPONENT_CODE,
            {{ transform_numeric('RES_ID') }} AS RES_ID,
            {{ transform_string('COMPONENT_TYPE') }} AS COMPONENT_TYPE,
            {{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
            {{ transform_numeric('RES_PACKAGE_ID') }} AS RES_PACKAGE_ID,
            {{ transform_string('COMPONENT_SUBCODE1') }} AS COMPONENT_SUBCODE1,
            {{ transform_string('COMPONENT_SUBCODE2') }} AS COMPONENT_SUBCODE2,
            {{ transform_string('COMPONENT_SUBCODE3') }} AS COMPONENT_SUBCODE3,
            {{ transform_string('LOCATION_TYPE_FROM') }} AS LOCATION_TYPE_FROM,
            {{ transform_string('LOCATION_CODE_FROM') }} AS LOCATION_CODE_FROM,
            {{ transform_string('LOCATION_TYPE_TO') }} AS LOCATION_TYPE_TO,
            {{ transform_string('LOCATION_CODE_TO') }} AS LOCATION_CODE_TO,
            {{ transform_numeric('ITIN_RECORD_ID') }} AS ITIN_RECORD_ID,
            {{ transform_numeric('MACRO_PACKAGE_ID') }} AS MACRO_PACKAGE_ID,
            {{ transform_numeric('MACRO_PACKAGE_SEQN') }} AS MACRO_PACKAGE_SEQN,
            {{ transform_numeric('PARENT_PACKAGE_ID') }} AS PARENT_PACKAGE_ID,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'RES_COMPONENT') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["RES_COMPONENT_ID", "DATA_SOURCE"]) }} AS RES_COMPONENT_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["RES_COMPONENT_ID", "DATA_SOURCE"]) }} AS RES_COMPONENT_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

