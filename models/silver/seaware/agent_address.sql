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
        unique_key=['AGENT_ADDR_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'AGENT_ADDRESS') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'AGENT_ADDRESS') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'AGENT_ADDRESS', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'AGENT_ADDRESS') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'AGENT_ADDRESS', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'AGENT_ADDRESS') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'AGENT_ADDRESS') %}
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
            {{ transform_numeric('AGENT_ID') }} AS AGENT_ID,
            {{ transform_numeric('SEQ_NUMBER') }} AS SEQ_NUMBER,
            {{ transform_string('ADDRESS_TYPE') }} AS ADDRESS_TYPE,
            {{ transform_string('ADDRESS_LINE1') }} AS ADDRESS_LINE1,
            {{ transform_string('ADDRESS_LINE2') }} AS ADDRESS_LINE2,
            {{ transform_string('ADDRESS_CITY') }} AS ADDRESS_CITY,
            {{ transform_string('STATE_CODE') }} AS STATE_CODE,
            {{ transform_string('ZIP') }} AS ZIP,
            {{ transform_string('COUNTRY_CODE') }} AS COUNTRY_CODE,
            {{ transform_string('IS_ADDRESS_MAILING') }} AS IS_ADDRESS_MAILING,
            {{ transform_string('IS_ADDRESS_SHIPPING') }} AS IS_ADDRESS_SHIPPING,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_numeric('AGENT_ADDR_ID') }} AS AGENT_ADDR_ID,
            {{ transform_string('ADDRESS_LINE3') }} AS ADDRESS_LINE3,
            {{ transform_string('ADDRESS_LINE4') }} AS ADDRESS_LINE4,
            NULL AS DATE_FROM,
            NULL AS DATE_TO,
            NULL AS ARE_DATES_EVERY_YEAR,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'AGENT_ADDRESS') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('AGENT_ID') }} AS AGENT_ID,
            {{ transform_numeric('SEQ_NUMBER') }} AS SEQ_NUMBER,
            {{ transform_string('ADDRESS_TYPE') }} AS ADDRESS_TYPE,
            {{ transform_string('ADDRESS_LINE1') }} AS ADDRESS_LINE1,
            {{ transform_string('ADDRESS_LINE2') }} AS ADDRESS_LINE2,
            {{ transform_string('ADDRESS_CITY') }} AS ADDRESS_CITY,
            {{ transform_string('STATE_CODE') }} AS STATE_CODE,
            {{ transform_string('ZIP') }} AS ZIP,
            {{ transform_string('COUNTRY_CODE') }} AS COUNTRY_CODE,
            {{ transform_string('IS_ADDRESS_MAILING') }} AS IS_ADDRESS_MAILING,
            {{ transform_string('IS_ADDRESS_SHIPPING') }} AS IS_ADDRESS_SHIPPING,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_numeric('AGENT_ADDR_ID') }} AS AGENT_ADDR_ID,
            {{ transform_string('ADDRESS_LINE3') }} AS ADDRESS_LINE3,
            {{ transform_string('ADDRESS_LINE4') }} AS ADDRESS_LINE4,
            {{ transform_date('DATE_FROM') }} AS DATE_FROM,
            {{ transform_date('DATE_TO') }} AS DATE_TO,
            {{ transform_string('ARE_DATES_EVERY_YEAR') }} AS ARE_DATES_EVERY_YEAR,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'AGENT_ADDRESS') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["AGENT_ADDR_ID", "DATA_SOURCE"]) }} AS AGENT_ADDRESS_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["AGENT_ADDR_ID", "DATA_SOURCE"]) }} AS AGENT_ADDRESS_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

