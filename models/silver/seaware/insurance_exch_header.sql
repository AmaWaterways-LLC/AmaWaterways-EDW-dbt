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
        unique_key=['EXCH_HEADER_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'INSURANCE_EXCH_HEADER') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'INSURANCE_EXCH_HEADER') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'INSURANCE_EXCH_HEADER', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'INSURANCE_EXCH_HEADER') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'INSURANCE_EXCH_HEADER', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'INSURANCE_EXCH_HEADER') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'INSURANCE_EXCH_HEADER') %}
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
            {{ transform_numeric('EXCH_HEADER_ID') }} AS EXCH_HEADER_ID,
            {{ transform_numeric('RES_ID') }} AS RES_ID,
            {{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
            {{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
            {{ transform_string('INSUR_EXCH_STATUS') }} AS INSUR_EXCH_STATUS,
            {{ transform_string('CANCELATION_TYPE') }} AS CANCELATION_TYPE,
            {{ transform_datetime('CANCELATION_DATE') }} AS CANCELATION_DATE,
            {{ transform_string('INSURANCE_TYPE') }} AS INSURANCE_TYPE,
            {{ transform_numeric('PREMIUM_AMOUNT') }} AS PREMIUM_AMOUNT,
            {{ transform_numeric('PENALTY_AMOUNT') }} AS PENALTY_AMOUNT,
            {{ transform_numeric('INVOICE_TOTAL') }} AS INVOICE_TOTAL,
            {{ transform_date('CLAIM_DATE') }} AS CLAIM_DATE,
            {{ transform_string('CLAIM_NUMBER') }} AS CLAIM_NUMBER,
            {{ transform_string('CLAIM_STATUS') }} AS CLAIM_STATUS,
            {{ transform_numeric('COMMISSIONS') }} AS COMMISSIONS,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_datetime('SUBMISSION_DATE') }} AS SUBMISSION_DATE,
            {{ transform_string('SUBMISSION_DONE') }} AS SUBMISSION_DONE,
            {{ transform_date('COUPON_DATE') }} AS COUPON_DATE,
            {{ transform_numeric('COUPON_NUMBER') }} AS COUPON_NUMBER,
            {{ transform_numeric('INSUR_COMPANY_SHARE') }} AS INSUR_COMPANY_SHARE,
            {{ transform_datetime('INSUR_CONFIRM_DATE') }} AS INSUR_CONFIRM_DATE,
            {{ transform_numeric('AGE') }} AS AGE,
            {{ transform_string('FULLY_PAID') }} AS FULLY_PAID,
            {{ transform_datetime('INSUR_CHANGE_DATE') }} AS INSUR_CHANGE_DATE,
            {{ transform_string('INSUR_CHANGE_DETAILS') }} AS INSUR_CHANGE_DETAILS,
            {{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
            {{ transform_date('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
            {{ transform_date('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
            {{ transform_datetime('VACATION_DATE_FROM') }} AS VACATION_DATE_FROM,
            {{ transform_date('VACATION_DATE_TO') }} AS VACATION_DATE_TO,
            {{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'INSURANCE_EXCH_HEADER') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full_sw1 %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["EXCH_HEADER_ID", "DATA_SOURCE"]) }} AS INSURANCE_EXCH_HEADER_SURROGATE_KEY,
    src.*
FROM src

