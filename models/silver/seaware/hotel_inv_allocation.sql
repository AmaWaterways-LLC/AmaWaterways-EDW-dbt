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
        unique_key=['ALLOCATION_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'HOTEL_INV_ALLOCATION') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'HOTEL_INV_ALLOCATION') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'HOTEL_INV_ALLOCATION', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'HOTEL_INV_ALLOCATION') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'HOTEL_INV_ALLOCATION', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'HOTEL_INV_ALLOCATION') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'HOTEL_INV_ALLOCATION') %}
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
            {{ transform_numeric('ALLOCATION_ID') }} AS ALLOCATION_ID,
            {{ transform_string('ALLOCATION_OWNER_TYPE') }} AS ALLOCATION_OWNER_TYPE,
            {{ transform_numeric('ALLOCATION_OWNER_ID') }} AS ALLOCATION_OWNER_ID,
            {{ transform_datetime('ALLOCATION_TIMESTAMP') }} AS ALLOCATION_TIMESTAMP,
            {{ transform_string('OPERATOR_ID') }} AS OPERATOR_ID,
            {{ transform_string('INVENTORY_REQUEST_TYPE') }} AS INVENTORY_REQUEST_TYPE,
            {{ transform_numeric('DEPENDS_ON_ID') }} AS DEPENDS_ON_ID,
            {{ transform_numeric('SEQ_NUMBER') }} AS SEQ_NUMBER,
            {{ transform_numeric('PROBABILITY') }} AS PROBABILITY,
            {{ transform_string('INVENTORY_RESULT_TYPE') }} AS INVENTORY_RESULT_TYPE,
            {{ transform_string('HOTEL_ROOM_CATEGORY') }} AS HOTEL_ROOM_CATEGORY,
            {{ transform_string('HOTEL_CATEGORY') }} AS HOTEL_CATEGORY,
            {{ transform_numeric('HOTEL_ID') }} AS HOTEL_ID,
            {{ transform_date('CHECK_IN_DATE') }} AS CHECK_IN_DATE,
            {{ transform_date('CHECK_OUT_DATE') }} AS CHECK_OUT_DATE,
            {{ transform_string('HOTEL_ROOM_TYPE') }} AS HOTEL_ROOM_TYPE,
            {{ transform_numeric('PRIORITY') }} AS PRIORITY,
            {{ transform_numeric('OCCUPANCY') }} AS OCCUPANCY,
            {{ transform_numeric('DECLINE_REASON') }} AS DECLINE_REASON,
            {{ transform_date('RELEASE_TIMESTAMP') }} AS RELEASE_TIMESTAMP,
            {{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
            {{ transform_string('CITY_CODE') }} AS CITY_CODE,
            {{ transform_string('WTL_OWNER_FLAG') }} AS WTL_OWNER_FLAG,
            {{ transform_numeric('DLGT_RES_ID') }} AS DLGT_RES_ID,
            {{ transform_string('IS_DLGT_OBJ') }} AS IS_DLGT_OBJ,
            NULL AS HOTEL_SPACE_TYPE,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_string('HOTEL_REQUEST_TYPE') }} AS HOTEL_REQUEST_TYPE
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'HOTEL_INV_ALLOCATION') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('ALLOCATION_ID') }} AS ALLOCATION_ID,
            {{ transform_string('ALLOCATION_OWNER_TYPE') }} AS ALLOCATION_OWNER_TYPE,
            {{ transform_numeric('ALLOCATION_OWNER_ID') }} AS ALLOCATION_OWNER_ID,
            {{ transform_datetime('ALLOCATION_TIMESTAMP') }} AS ALLOCATION_TIMESTAMP,
            {{ transform_string('OPERATOR_ID') }} AS OPERATOR_ID,
            {{ transform_string('INVENTORY_REQUEST_TYPE') }} AS INVENTORY_REQUEST_TYPE,
            {{ transform_numeric('DEPENDS_ON_ID') }} AS DEPENDS_ON_ID,
            {{ transform_numeric('SEQ_NUMBER') }} AS SEQ_NUMBER,
            {{ transform_numeric('PROBABILITY') }} AS PROBABILITY,
            {{ transform_string('INVENTORY_RESULT_TYPE') }} AS INVENTORY_RESULT_TYPE,
            {{ transform_string('HOTEL_ROOM_CATEGORY') }} AS HOTEL_ROOM_CATEGORY,
            {{ transform_string('HOTEL_CATEGORY') }} AS HOTEL_CATEGORY,
            {{ transform_numeric('HOTEL_ID') }} AS HOTEL_ID,
            {{ transform_date('CHECK_IN_DATE') }} AS CHECK_IN_DATE,
            {{ transform_date('CHECK_OUT_DATE') }} AS CHECK_OUT_DATE,
            {{ transform_string('HOTEL_ROOM_TYPE') }} AS HOTEL_ROOM_TYPE,
            {{ transform_numeric('PRIORITY') }} AS PRIORITY,
            {{ transform_numeric('OCCUPANCY') }} AS OCCUPANCY,
            {{ transform_numeric('DECLINE_REASON') }} AS DECLINE_REASON,
            {{ transform_date('RELEASE_TIMESTAMP') }} AS RELEASE_TIMESTAMP,
            {{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
            {{ transform_string('CITY_CODE') }} AS CITY_CODE,
            {{ transform_string('WTL_OWNER_FLAG') }} AS WTL_OWNER_FLAG,
            {{ transform_numeric('DLGT_RES_ID') }} AS DLGT_RES_ID,
            {{ transform_string('IS_DLGT_OBJ') }} AS IS_DLGT_OBJ,
            {{ transform_string('HOTEL_SPACE_TYPE') }} AS HOTEL_SPACE_TYPE,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            NULL AS HOTEL_REQUEST_TYPE
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'HOTEL_INV_ALLOCATION') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["ALLOCATION_ID", "DATA_SOURCE"]) }} AS HOTEL_INV_ALLOCATION_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["ALLOCATION_ID", "DATA_SOURCE"]) }} AS HOTEL_INV_ALLOCATION_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

