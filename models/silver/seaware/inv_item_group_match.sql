{# ================================================================
   Generate Batch ID (must be top-of-file, before config)
   ================================================================ #}
{% set batch_id = invocation_id ~ '-' ~ this.name ~ '-' ~ modules.datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S%fZ') %}

{# ================================================================
   CONFIG BLOCK
   ================================================================ #}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['RECORD_ID', 'DATA_SOURCE'],

        pre_hook = [
            "{% set target_relation = adapter.get_relation(
                    database=this.database,
                    schema=this.schema,
                    identifier=this.name
            ) %}
             {% if target_relation is not none %}
                 {% set cfg = get_config_row(
                        'SW1',
                        this.database,
                        this.schema,
                        'INV_ITEM_GROUP_MATCH'
                 ) %}
             {% endif %}"
        ],

        post_hook = [
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column(
                        'SW1',
                        this.database,
                        this.schema,
                        'INV_ITEM_GROUP_MATCH'
                 ) %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(
                        this,
                        wm_col_sw1,
                        'SW1'
                 ) %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark(
                        'SW1',
                        this.database,
                        this.schema,
                        'INV_ITEM_GROUP_MATCH',
                        max_wm_sw1
                     ) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column(
                        'SW2',
                        this.database,
                        this.schema,
                        'INV_ITEM_GROUP_MATCH'
                 ) %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(
                        this,
                        wm_col_sw2,
                        'SW2'
                 ) %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark(
                        'SW2',
                        this.database,
                        this.schema,
                        'INV_ITEM_GROUP_MATCH',
                        max_wm_sw2
                     ) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'INV_ITEM_GROUP_MATCH') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}

    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'INV_ITEM_GROUP_MATCH') %}
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
   SOURCE CTEs
   ================================================================ #}

WITH sw1_src AS (
    SELECT
        'SW1' AS DATA_SOURCE,
        {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
        {{ transform_string('INVOICE_ITEM_TYPE') }} AS INVOICE_ITEM_TYPE,
        {{ transform_string('INV_ITEM_GROUP') }} AS INV_ITEM_GROUP,
        {{ transform_string('INV_ITEM_GROUP_TYPE') }} AS INV_ITEM_GROUP_TYPE,
        {{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
        {{ transform_string('COMMENTS') }} AS COMMENTS,
        {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
        _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'INV_ITEM_GROUP_MATCH') }}
    {% if is_incremental() and not is_full_sw1 %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }})
          > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
        'SW2' AS DATA_SOURCE,
        {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
        {{ transform_string('INVOICE_ITEM_TYPE') }} AS INVOICE_ITEM_TYPE,
        {{ transform_string('INV_ITEM_GROUP') }} AS INV_ITEM_GROUP,
        {{ transform_string('INV_ITEM_GROUP_TYPE') }} AS INV_ITEM_GROUP_TYPE,
        {{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
        {{ transform_string('COMMENTS') }} AS COMMENTS,
        {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
        _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'INV_ITEM_GROUP_MATCH') }}
    {% if is_incremental() and not is_full_sw2 %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }})
          > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

{# ================================================================
   FINAL SELECT
   ================================================================ #}

SELECT
    {{ dbt_utils.generate_surrogate_key(['RECORD_ID', 'DATA_SOURCE']) }}
        AS INV_ITEM_GROUP_MATCH_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['RECORD_ID', 'DATA_SOURCE']) }}
        AS INV_ITEM_GROUP_MATCH_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src
