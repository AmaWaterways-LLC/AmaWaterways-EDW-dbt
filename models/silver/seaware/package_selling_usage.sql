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
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'PACKAGE_SELLING_USAGE') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'PACKAGE_SELLING_USAGE') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'PACKAGE_SELLING_USAGE', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'PACKAGE_SELLING_USAGE') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'PACKAGE_SELLING_USAGE', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'PACKAGE_SELLING_USAGE') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'PACKAGE_SELLING_USAGE') %}
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
            {{ transform_numeric('LIMIT_RECORD_ID') }} AS LIMIT_RECORD_ID,
            {{ transform_string('ALLOCATION_OWNER_TYPE') }} AS ALLOCATION_OWNER_TYPE,
            {{ transform_numeric('ALLOCATION_OWNER_ID') }} AS ALLOCATION_OWNER_ID,
            {{ transform_string('OWNER_FLAG') }} AS OWNER_FLAG,
            {{ transform_datetime('ALLOCATION_TIMESTAMP') }} AS ALLOCATION_TIMESTAMP,
            {{ transform_numeric('PRIORITY') }} AS PRIORITY,
            {{ transform_numeric('USAGE_ABS') }} AS USAGE_ABS,
            {{ transform_numeric('USAGE_WGT') }} AS USAGE_WGT,
            {{ transform_string('INVENTORY_RESULT_TYPE') }} AS INVENTORY_RESULT_TYPE,
            {{ transform_datetime('RELEASE_OK_TIMESTAMP') }} AS RELEASE_OK_TIMESTAMP,
            {{ transform_string('INVENTORY_REQUEST_TYPE') }} AS INVENTORY_REQUEST_TYPE,
            {{ transform_numeric('ALLOCATION_SUBOWNER_ID') }} AS ALLOCATION_SUBOWNER_ID,
            {{ transform_numeric('SECONDARY_ID') }} AS SECONDARY_ID,
            {{ transform_string('WTL_OWNER_FLAG') }} AS WTL_OWNER_FLAG,
            {{ transform_numeric('RECURRENCE_ID') }} AS RECURRENCE_ID,
            NULL AS IS_IN_GROUP,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source(var('bronze_source_prefix') ~ '_SW1', 'PACKAGE_SELLING_USAGE') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('ALLOCATION_ID') }} AS ALLOCATION_ID,
            {{ transform_numeric('LIMIT_RECORD_ID') }} AS LIMIT_RECORD_ID,
            {{ transform_string('ALLOCATION_OWNER_TYPE') }} AS ALLOCATION_OWNER_TYPE,
            {{ transform_numeric('ALLOCATION_OWNER_ID') }} AS ALLOCATION_OWNER_ID,
            {{ transform_string('OWNER_FLAG') }} AS OWNER_FLAG,
            {{ transform_datetime('ALLOCATION_TIMESTAMP') }} AS ALLOCATION_TIMESTAMP,
            {{ transform_numeric('PRIORITY') }} AS PRIORITY,
            {{ transform_numeric('USAGE_ABS') }} AS USAGE_ABS,
            {{ transform_numeric('USAGE_WGT') }} AS USAGE_WGT,
            {{ transform_string('INVENTORY_RESULT_TYPE') }} AS INVENTORY_RESULT_TYPE,
            {{ transform_date('RELEASE_OK_TIMESTAMP') }} AS RELEASE_OK_TIMESTAMP,
            {{ transform_string('INVENTORY_REQUEST_TYPE') }} AS INVENTORY_REQUEST_TYPE,
            {{ transform_numeric('ALLOCATION_SUBOWNER_ID') }} AS ALLOCATION_SUBOWNER_ID,
            {{ transform_numeric('SECONDARY_ID') }} AS SECONDARY_ID,
            {{ transform_string('WTL_OWNER_FLAG') }} AS WTL_OWNER_FLAG,
            {{ transform_numeric('RECURRENCE_ID') }} AS RECURRENCE_ID,
            {{ transform_string('IS_IN_GROUP') }} AS IS_IN_GROUP,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source(var('bronze_source_prefix') ~ '_SW2', 'PACKAGE_SELLING_USAGE') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT *
FROM (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["ALLOCATION_ID", "DATA_SOURCE"]) }} AS PACKAGE_SELLING_USAGE_SURROGATE_KEY,
        sw1_src.*
    FROM sw1_src

    UNION ALL

    SELECT
        {{ dbt_utils.generate_surrogate_key(["ALLOCATION_ID", "DATA_SOURCE"]) }} AS PACKAGE_SELLING_USAGE_SURROGATE_KEY,
        sw2_src.*
    FROM sw2_src
)
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY ALLOCATION_ID, DATA_SOURCE
        ORDER BY LAST_UPDATED_TIMESTAMP DESC
) = 1

