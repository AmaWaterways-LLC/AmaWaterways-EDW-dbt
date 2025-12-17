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
        unique_key=['HOTEL_REQUEST_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'RES_HOTEL_REQUEST') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'RES_HOTEL_REQUEST') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'RES_HOTEL_REQUEST', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'RES_HOTEL_REQUEST') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'RES_HOTEL_REQUEST', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'RES_HOTEL_REQUEST') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'RES_HOTEL_REQUEST') %}
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
            {{ transform_numeric('HOTEL_REQUEST_ID') }} AS HOTEL_REQUEST_ID,
            {{ transform_numeric('RES_ID') }} AS RES_ID,
            {{ transform_numeric('ROOM_SEQ_NUMBER') }} AS ROOM_SEQ_NUMBER,
            {{ transform_date('DATE_FROM') }} AS DATE_FROM,
            {{ transform_date('DATE_TO') }} AS DATE_TO,
            {{ transform_string('HOTEL_REQUEST_TYPE') }} AS HOTEL_REQUEST_TYPE,
            {{ transform_string('HOTEL_CATEGORY') }} AS HOTEL_CATEGORY,
            {{ transform_string('HOTEL_ROOM_CATEGORY') }} AS HOTEL_ROOM_CATEGORY,
            {{ transform_numeric('HOTEL_ID') }} AS HOTEL_ID,
            {{ transform_string('HOTEL_ROOM_TYPE') }} AS HOTEL_ROOM_TYPE,
            {{ transform_numeric('ALLOCATION_ID') }} AS ALLOCATION_ID,
            {{ transform_string('INVENTORY_REQUEST_TYPE') }} AS INVENTORY_REQUEST_TYPE,
            {{ transform_string('INVENTORY_RESULT_TYPE') }} AS INVENTORY_RESULT_TYPE,
            {{ transform_string('HOTEL_CATEGORY_PACKAGE') }} AS HOTEL_CATEGORY_PACKAGE,
            {{ transform_numeric('OCCUPANCY') }} AS OCCUPANCY,
            {{ transform_string('CITY_CODE') }} AS CITY_CODE,
            {{ transform_numeric('PACKAGE_ID') }} AS PACKAGE_ID,
            NULL AS HOTEL_SPACE_TYPE,
            NULL AS SELL_LIM_INV_RESULT,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'RES_HOTEL_REQUEST') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('HOTEL_REQUEST_ID') }} AS HOTEL_REQUEST_ID,
            {{ transform_numeric('RES_ID') }} AS RES_ID,
            {{ transform_numeric('ROOM_SEQ_NUMBER') }} AS ROOM_SEQ_NUMBER,
            {{ transform_date('DATE_FROM') }} AS DATE_FROM,
            {{ transform_date('DATE_TO') }} AS DATE_TO,
            {{ transform_string('HOTEL_REQUEST_TYPE') }} AS HOTEL_REQUEST_TYPE,
            {{ transform_string('HOTEL_CATEGORY') }} AS HOTEL_CATEGORY,
            {{ transform_string('HOTEL_ROOM_CATEGORY') }} AS HOTEL_ROOM_CATEGORY,
            {{ transform_numeric('HOTEL_ID') }} AS HOTEL_ID,
            {{ transform_string('HOTEL_ROOM_TYPE') }} AS HOTEL_ROOM_TYPE,
            {{ transform_numeric('ALLOCATION_ID') }} AS ALLOCATION_ID,
            {{ transform_string('INVENTORY_REQUEST_TYPE') }} AS INVENTORY_REQUEST_TYPE,
            {{ transform_string('INVENTORY_RESULT_TYPE') }} AS INVENTORY_RESULT_TYPE,
            {{ transform_string('HOTEL_CATEGORY_PACKAGE') }} AS HOTEL_CATEGORY_PACKAGE,
            {{ transform_numeric('OCCUPANCY') }} AS OCCUPANCY,
            {{ transform_string('CITY_CODE') }} AS CITY_CODE,
            {{ transform_numeric('PACKAGE_ID') }} AS PACKAGE_ID,
            {{ transform_string('HOTEL_SPACE_TYPE') }} AS HOTEL_SPACE_TYPE,
            {{ transform_string('SELL_LIM_INV_RESULT') }} AS SELL_LIM_INV_RESULT,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'RES_HOTEL_REQUEST') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["HOTEL_REQUEST_ID", "DATA_SOURCE"]) }} AS RES_HOTEL_REQUEST_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["HOTEL_REQUEST_ID", "DATA_SOURCE"]) }} AS RES_HOTEL_REQUEST_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

