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
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'RES_CHARGE_SUBDETAIL') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'RES_CHARGE_SUBDETAIL') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'RES_CHARGE_SUBDETAIL', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'RES_CHARGE_SUBDETAIL') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'RES_CHARGE_SUBDETAIL', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'RES_CHARGE_SUBDETAIL') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'RES_CHARGE_SUBDETAIL') %}
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
            {{ transform_numeric('GUEST_SEQN') }} AS GUEST_SEQN,
            {{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
            {{ transform_numeric('AMOUNT') }} AS AMOUNT,
            {{ transform_string('EXPLANATION') }} AS EXPLANATION,
            {{ transform_numeric('CHARGE_ID') }} AS CHARGE_ID,
            {{ transform_string('CHANGE_AREA') }} AS CHANGE_AREA,
            {{ transform_string('ORIGIN') }} AS ORIGIN,
            {{ transform_string('BASIS') }} AS BASIS,
            NULL AS PACKAGE_ID,
            NULL AS INVOICE_ITEM_TYPE,
            NULL AS PERCENT,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source(var('bronze_source_prefix') ~ '_SW1', 'RES_CHARGE_SUBDETAIL') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
            {{ transform_numeric('GUEST_SEQN') }} AS GUEST_SEQN,
            {{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
            {{ transform_numeric('AMOUNT') }} AS AMOUNT,
            {{ transform_string('EXPLANATION') }} AS EXPLANATION,
            {{ transform_numeric('CHARGE_ID') }} AS CHARGE_ID,
            {{ transform_string('CHANGE_AREA') }} AS CHANGE_AREA,
            {{ transform_string('ORIGIN') }} AS ORIGIN,
            {{ transform_string('BASIS') }} AS BASIS,
            {{ transform_numeric('PACKAGE_ID') }} AS PACKAGE_ID,
            {{ transform_string('INVOICE_ITEM_TYPE') }} AS INVOICE_ITEM_TYPE,
            {{ transform_numeric('PERCENT') }} AS PERCENT,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source(var('bronze_source_prefix') ~ '_SW2', 'RES_CHARGE_SUBDETAIL') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT *
FROM (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["RECORD_ID", "DATA_SOURCE"]) }} AS RES_CHARGE_SUBDETAIL_SURROGATE_KEY,
        sw1_src.*
    FROM sw1_src

    UNION ALL

    SELECT
        {{ dbt_utils.generate_surrogate_key(["RECORD_ID", "DATA_SOURCE"]) }} AS RES_CHARGE_SUBDETAIL_SURROGATE_KEY,
        sw2_src.*
    FROM sw2_src
)
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY RECORD_ID, DATA_SOURCE
        ORDER BY LAST_UPDATED_TIMESTAMP DESC
) = 1

