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
        unique_key=['REQUEST_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'GTB_REQUEST') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'GTB_REQUEST') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'GTB_REQUEST', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'GTB_REQUEST') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'GTB_REQUEST', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'GTB_REQUEST') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'GTB_REQUEST') %}
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
            {{ transform_numeric('REQUEST_ID') }} AS REQUEST_ID,
            {{ transform_date('AIR_LAST_CHECK_DATE') }} AS AIR_LAST_CHECK_DATE,
            {{ transform_date('AIR_GOT_DATE') }} AS AIR_GOT_DATE,
            {{ transform_string('BATCH_CODE') }} AS BATCH_CODE,
            {{ transform_numeric('BOOKLET_ID') }} AS BOOKLET_ID,
            {{ transform_string('ERROR_MSG') }} AS ERROR_MSG,
            {{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
            {{ transform_datetime('PRINT_DATE') }} AS PRINT_DATE,
            {{ transform_datetime('REQUEST_DATE') }} AS REQUEST_DATE,
            {{ transform_string('REQUEST_STATUS') }} AS REQUEST_STATUS,
            {{ transform_string('REQUEST_TYPE') }} AS REQUEST_TYPE,
            {{ transform_numeric('RES_ID') }} AS RES_ID,
            {{ transform_datetime('VACATION_DATE') }} AS VACATION_DATE,
            {{ transform_string('FORCED_BY_USER') }} AS FORCED_BY_USER,
            {{ transform_numeric('TKT_AUTH_ID') }} AS TKT_AUTH_ID,
            {{ transform_string('BARCODE') }} AS BARCODE,
            {{ transform_string('MAILING_NAME') }} AS MAILING_NAME,
            {{ transform_string('MAILING_ADDRESS_LINE1') }} AS MAILING_ADDRESS_LINE1,
            {{ transform_string('MAILING_ADDRESS_LINE2') }} AS MAILING_ADDRESS_LINE2,
            {{ transform_string('MAILING_CITY') }} AS MAILING_CITY,
            {{ transform_string('MAILING_STATE') }} AS MAILING_STATE,
            {{ transform_string('MAILING_ZIP') }} AS MAILING_ZIP,
            {{ transform_string('MAILING_COUNTRY') }} AS MAILING_COUNTRY,
            {{ transform_string('MAILING_PHONE') }} AS MAILING_PHONE,
            {{ transform_string('FEDEX_TRACK_NUM') }} AS FEDEX_TRACK_NUM,
            {{ transform_string('FEDEX_SHIP_DATE') }} AS FEDEX_SHIP_DATE,
            {{ transform_string('FEDEX_SERV_TYPE') }} AS FEDEX_SERV_TYPE,
            {{ transform_string('LANGUAGE_CODE') }} AS LANGUAGE_CODE,
            {{ transform_string('CARRIER_NAME') }} AS CARRIER_NAME,
            {{ transform_string('TRACK_NUMBER') }} AS TRACK_NUMBER,
            {{ transform_date('SHIPPING_DATE') }} AS SHIPPING_DATE,
            {{ transform_string('ADVANCED_DELIVERY_FEEDBACK') }} AS ADVANCED_DELIVERY_FEEDBACK,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'GTB_REQUEST') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full_sw1 %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["REQUEST_ID", "DATA_SOURCE"]) }} AS GTB_REQUEST_SURROGATE_KEY,
    src.*
FROM src

