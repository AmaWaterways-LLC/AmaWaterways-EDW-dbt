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
        unique_key=['AMENITY_CODE', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'AM_CODE') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'AM_CODE') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'AM_CODE', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'AM_CODE') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'AM_CODE', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'AM_CODE') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'AM_CODE') %}
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
            {{ transform_string('AMENITY_CODE') }} AS AMENITY_CODE,
            {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
            {{ transform_string('AM_CATEGORY') }} AS AM_CATEGORY,
            {{ transform_string('AMENITY_TYPE') }} AS AMENITY_TYPE,
            {{ transform_string('PROVIDER_CODE') }} AS PROVIDER_CODE,
            {{ transform_string('AMENITY_NAME') }} AS AMENITY_NAME,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('USE_TAX') }} AS USE_TAX,
            {{ transform_string('SUBCODE_MANDATORY') }} AS SUBCODE_MANDATORY,
            {{ transform_string('MANDATORY_BEFORE_DLV') }} AS MANDATORY_BEFORE_DLV,
            {{ transform_string('CODE_ACTIVE') }} AS CODE_ACTIVE,
            {{ transform_string('ADDON_RESTRICTED') }} AS ADDON_RESTRICTED,
            {{ transform_string('ADDON_ORDER_BASIS') }} AS ADDON_ORDER_BASIS,
            {{ transform_string('ADDON_INV_ITEM_TYPE') }} AS ADDON_INV_ITEM_TYPE,
            {{ transform_string('INCLUDE_IN_ORDER') }} AS INCLUDE_IN_ORDER,
            NULL AS INITIAL_STATUS,
            NULL AS LINKED_CLUB_TYPE,
            NULL AS LINKED_CLUB_LEVEL,
            NULL AS COUPON_CLASS,
            NULL AS MANUAL_PRICE,
            NULL AS USE_AS_CLIENT_PREFER,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_string('ADDON_COMPONENT_TYPE') }} AS ADDON_COMPONENT_TYPE,
            {{ transform_numeric('AGE_FROM') }} AS AGE_FROM,
            {{ transform_numeric('AGE_TO') }} AS AGE_TO
    FROM {{ source(var('bronze_source_prefix') ~ '_SW1', 'AM_CODE') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_string('AMENITY_CODE') }} AS AMENITY_CODE,
            {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
            {{ transform_string('AM_CATEGORY') }} AS AM_CATEGORY,
            {{ transform_string('AMENITY_TYPE') }} AS AMENITY_TYPE,
            {{ transform_string('PROVIDER_CODE') }} AS PROVIDER_CODE,
            {{ transform_string('AMENITY_NAME') }} AS AMENITY_NAME,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('USE_TAX') }} AS USE_TAX,
            {{ transform_string('SUBCODE_MANDATORY') }} AS SUBCODE_MANDATORY,
            {{ transform_string('MANDATORY_BEFORE_DLV') }} AS MANDATORY_BEFORE_DLV,
            {{ transform_string('CODE_ACTIVE') }} AS CODE_ACTIVE,
            {{ transform_string('ADDON_RESTRICTED') }} AS ADDON_RESTRICTED,
            {{ transform_string('ADDON_ORDER_BASIS') }} AS ADDON_ORDER_BASIS,
            {{ transform_string('ADDON_INV_ITEM_TYPE') }} AS ADDON_INV_ITEM_TYPE,
            {{ transform_string('INCLUDE_IN_ORDER') }} AS INCLUDE_IN_ORDER,
            {{ transform_string('INITIAL_STATUS') }} AS INITIAL_STATUS,
            {{ transform_string('LINKED_CLUB_TYPE') }} AS LINKED_CLUB_TYPE,
            {{ transform_string('LINKED_CLUB_LEVEL') }} AS LINKED_CLUB_LEVEL,
            {{ transform_string('COUPON_CLASS') }} AS COUPON_CLASS,
            {{ transform_string('MANUAL_PRICE') }} AS MANUAL_PRICE,
            {{ transform_string('USE_AS_CLIENT_PREFER') }} AS USE_AS_CLIENT_PREFER,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            NULL AS ADDON_COMPONENT_TYPE,
            NULL AS AGE_FROM,
            NULL AS AGE_TO
    FROM {{ source(var('bronze_source_prefix') ~ '_SW2', 'AM_CODE') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT *
FROM (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["AMENITY_CODE", "DATA_SOURCE"]) }} AS AM_CODE_SURROGATE_KEY,
        sw1_src.*
    FROM sw1_src

    UNION ALL

    SELECT
        {{ dbt_utils.generate_surrogate_key(["AMENITY_CODE", "DATA_SOURCE"]) }} AS AM_CODE_SURROGATE_KEY,
        sw2_src.*
    FROM sw2_src
)
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY AMENITY_CODE, DATA_SOURCE
        ORDER BY LAST_UPDATED_TIMESTAMP DESC
) = 1

