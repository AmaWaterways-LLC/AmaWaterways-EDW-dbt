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
        unique_key=['TRANS_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'ACC_TRANS_DETAIL') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'ACC_TRANS_DETAIL') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'ACC_TRANS_DETAIL', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'ACC_TRANS_DETAIL') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'ACC_TRANS_DETAIL', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'ACC_TRANS_DETAIL') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'ACC_TRANS_DETAIL') %}
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
            {{ transform_numeric('TRANS_ID') }} AS TRANS_ID,
            {{ transform_string('TRANS_STATUS') }} AS TRANS_STATUS,
            {{ transform_string('FORM_OF_TRANS') }} AS FORM_OF_TRANS,
            {{ transform_string('TRANS_TYPE') }} AS TRANS_TYPE,
            {{ transform_string('SOURCE_ENTITY_TYPE') }} AS SOURCE_ENTITY_TYPE,
            {{ transform_numeric('SOURCE_ENTITY_ID') }} AS SOURCE_ENTITY_ID,
            {{ transform_string('DEST_ENTITY_TYPE') }} AS DEST_ENTITY_TYPE,
            {{ transform_numeric('DEST_ENTITY_ID') }} AS DEST_ENTITY_ID,
            {{ transform_string('EXTERNAL_IDENT') }} AS EXTERNAL_IDENT,
            {{ transform_numeric('BATCH_HDR_ID') }} AS BATCH_HDR_ID,
            {{ transform_datetime('TRANS_TIME_STAMP') }} AS TRANS_TIME_STAMP,
            {{ transform_numeric('TRANS_GROUP_ID') }} AS TRANS_GROUP_ID,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('USER_ID') }} AS USER_ID,
            {{ transform_datetime('VOIDED_TIME_STAMP') }} AS VOIDED_TIME_STAMP,
            {{ transform_string('VOIDED_BY_USER_ID') }} AS VOIDED_BY_USER_ID,
            {{ transform_datetime('APPROVED_TIME_STAMP') }} AS APPROVED_TIME_STAMP,
            {{ transform_string('APPROVED_BY_USER_ID') }} AS APPROVED_BY_USER_ID,
            {{ transform_date('TRANS_STATUS_TIME_STAMP') }} AS TRANS_STATUS_TIME_STAMP,
            {{ transform_string('USE_EXTENDED_PAY') }} AS USE_EXTENDED_PAY,
            {{ transform_date('DUE_DATE') }} AS DUE_DATE,
            {{ transform_string('INSUR_PAID') }} AS INSUR_PAID,
            {{ transform_string('CC_AUTH_CODE') }} AS CC_AUTH_CODE,
            {{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
            {{ transform_numeric('AR_AP_SEQ_ID') }} AS AR_AP_SEQ_ID,
            {{ transform_date('AR_AP_TIME_STAMP') }} AS AR_AP_TIME_STAMP,
            {{ transform_date('AR_AP_DUE_DATE') }} AS AR_AP_DUE_DATE,
            {{ transform_string('MERCHANT_NUMBER') }} AS MERCHANT_NUMBER,
            {{ transform_numeric('ACT_HIST_ID') }} AS ACT_HIST_ID,
            {{ transform_numeric('FPR_ID') }} AS FPR_ID,
            {{ transform_string('OFFICE_LOCATION') }} AS OFFICE_LOCATION,
            {{ transform_string('IS_MANUAL') }} AS IS_MANUAL,
            NULL AS AMOUNT_FULL_TRANS,
            NULL AS AMOUNT_FULL_PRICE,
            NULL AS CURRENCY_TRANS,
            NULL AS CURRENCY_PRICE,
            NULL AS INVOICED_SEPARATELY,
            NULL AS BASE_ENTITY_TYPE,
            NULL AS BASE_ENTITY_ID,
            NULL AS GROUPING_LINE,
            {{ transform_numeric('CC_CLIENT_ID') }} AS CC_CLIENT_ID,
            NULL AS TERMS_CODE,
            NULL AS AFTER_FPR_ID,
            NULL AS PREV_INVOICE_ID,
            NULL AS DISTRIBUTION_CHANNEL,
            NULL AS SINGLE_INVOICED,
            NULL AS COMMISS_PAID,
            NULL AS PMNT_FOR_CLIENT_ID,
            NULL AS GROSS_UP,
            NULL AS PAYMENT_TRANS_ID,
            NULL AS SERVER_RESPONSE,
            NULL AS NOT_REFUNDABLE,
            NULL AS SEC_TRANS_ID,
            NULL AS TRANS_LINK_TYPE,
            NULL AS CC_SERVER_TYPE,
            NULL AS GL_RATE_DATE,
            NULL AS TO_BE_BALANCED,
            NULL AS SERVER_RESPONSE_LONG,
            {{ transform_string('EXCH_RATE_TYPE') }} AS EXCH_RATE_TYPE,
            NULL AS POST_DATE,
            NULL AS BANK_ID,
            NULL AS LAST_GL_DATE,
            NULL AS AMOUNT_FULL_BASE,
            NULL AS USER_TYPE,
            NULL AS USER_CLIENT_ID,
            NULL AS USER_AGENT_ID,
            NULL AS BANK_FEE,
            NULL AS EXCHANGE_GAIN,
            NULL AS TRANS_GUID,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            NULL AS RATE_PRICEBASE,
            NULL AS RATE_TRANSPRICE,
            {{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
            {{ transform_numeric('TRANS_AMOUNT') }} AS TRANS_AMOUNT,
            {{ transform_numeric('TRANS_AMOUNT_LEFT') }} AS TRANS_AMOUNT_LEFT,
            {{ transform_numeric('AMOUNT_ORIG_CURRENCY') }} AS AMOUNT_ORIG_CURRENCY,
            {{ transform_numeric('FIRST_TRANS_ID') }} AS FIRST_TRANS_ID,
            {{ transform_numeric('PREV_TRANS_ID') }} AS PREV_TRANS_ID,
            {{ transform_numeric('AMOUNT_ORIG_CURRENCY_LEFT') }} AS AMOUNT_ORIG_CURRENCY_LEFT,
            {{ transform_numeric('EXCHANGE_RATE') }} AS EXCHANGE_RATE,
            {{ transform_numeric('REF_TRANS_ID') }} AS REF_TRANS_ID,
            {{ transform_string('INTERN_BALANCED_COM') }} AS INTERN_BALANCED_COM
    FROM {{ source(var('bronze_source_prefix') ~ '_SW1', 'ACC_TRANS_DETAIL') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('TRANS_ID') }} AS TRANS_ID,
            {{ transform_string('TRANS_STATUS') }} AS TRANS_STATUS,
            {{ transform_string('FORM_OF_TRANS') }} AS FORM_OF_TRANS,
            {{ transform_string('TRANS_TYPE') }} AS TRANS_TYPE,
            {{ transform_string('SOURCE_ENTITY_TYPE') }} AS SOURCE_ENTITY_TYPE,
            {{ transform_numeric('SOURCE_ENTITY_ID') }} AS SOURCE_ENTITY_ID,
            {{ transform_string('DEST_ENTITY_TYPE') }} AS DEST_ENTITY_TYPE,
            {{ transform_numeric('DEST_ENTITY_ID') }} AS DEST_ENTITY_ID,
            {{ transform_string('EXTERNAL_IDENT') }} AS EXTERNAL_IDENT,
            {{ transform_numeric('BATCH_HDR_ID') }} AS BATCH_HDR_ID,
            {{ transform_datetime('TRANS_TIME_STAMP') }} AS TRANS_TIME_STAMP,
            {{ transform_numeric('TRANS_GROUP_ID') }} AS TRANS_GROUP_ID,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('USER_ID') }} AS USER_ID,
            {{ transform_datetime('VOIDED_TIME_STAMP') }} AS VOIDED_TIME_STAMP,
            {{ transform_string('VOIDED_BY_USER_ID') }} AS VOIDED_BY_USER_ID,
            {{ transform_datetime('APPROVED_TIME_STAMP') }} AS APPROVED_TIME_STAMP,
            {{ transform_string('APPROVED_BY_USER_ID') }} AS APPROVED_BY_USER_ID,
            {{ transform_date('TRANS_STATUS_TIME_STAMP') }} AS TRANS_STATUS_TIME_STAMP,
            {{ transform_string('USE_EXTENDED_PAY') }} AS USE_EXTENDED_PAY,
            {{ transform_date('DUE_DATE') }} AS DUE_DATE,
            {{ transform_string('INSUR_PAID') }} AS INSUR_PAID,
            {{ transform_string('CC_AUTH_CODE') }} AS CC_AUTH_CODE,
            {{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
            {{ transform_numeric('AR_AP_SEQ_ID') }} AS AR_AP_SEQ_ID,
            {{ transform_date('AR_AP_TIME_STAMP') }} AS AR_AP_TIME_STAMP,
            {{ transform_date('AR_AP_DUE_DATE') }} AS AR_AP_DUE_DATE,
            {{ transform_string('MERCHANT_NUMBER') }} AS MERCHANT_NUMBER,
            {{ transform_numeric('ACT_HIST_ID') }} AS ACT_HIST_ID,
            {{ transform_numeric('FPR_ID') }} AS FPR_ID,
            {{ transform_string('OFFICE_LOCATION') }} AS OFFICE_LOCATION,
            {{ transform_string('IS_MANUAL') }} AS IS_MANUAL,
            {{ transform_numeric('AMOUNT_FULL_TRANS') }} AS AMOUNT_FULL_TRANS,
            {{ transform_numeric('AMOUNT_FULL_PRICE') }} AS AMOUNT_FULL_PRICE,
            {{ transform_string('CURRENCY_TRANS') }} AS CURRENCY_TRANS,
            {{ transform_string('CURRENCY_PRICE') }} AS CURRENCY_PRICE,
            {{ transform_string('INVOICED_SEPARATELY') }} AS INVOICED_SEPARATELY,
            {{ transform_string('BASE_ENTITY_TYPE') }} AS BASE_ENTITY_TYPE,
            {{ transform_numeric('BASE_ENTITY_ID') }} AS BASE_ENTITY_ID,
            {{ transform_string('GROUPING_LINE') }} AS GROUPING_LINE,
            {{ transform_numeric('CC_CLIENT_ID') }} AS CC_CLIENT_ID,
            {{ transform_string('TERMS_CODE') }} AS TERMS_CODE,
            {{ transform_numeric('AFTER_FPR_ID') }} AS AFTER_FPR_ID,
            {{ transform_numeric('PREV_INVOICE_ID') }} AS PREV_INVOICE_ID,
            {{ transform_string('DISTRIBUTION_CHANNEL') }} AS DISTRIBUTION_CHANNEL,
            {{ transform_string('SINGLE_INVOICED') }} AS SINGLE_INVOICED,
            {{ transform_numeric('COMMISS_PAID') }} AS COMMISS_PAID,
            {{ transform_numeric('PMNT_FOR_CLIENT_ID') }} AS PMNT_FOR_CLIENT_ID,
            {{ transform_string('GROSS_UP') }} AS GROSS_UP,
            {{ transform_numeric('PAYMENT_TRANS_ID') }} AS PAYMENT_TRANS_ID,
            {{ transform_string('SERVER_RESPONSE') }} AS SERVER_RESPONSE,
            {{ transform_string('NOT_REFUNDABLE') }} AS NOT_REFUNDABLE,
            {{ transform_numeric('SEC_TRANS_ID') }} AS SEC_TRANS_ID,
            {{ transform_string('TRANS_LINK_TYPE') }} AS TRANS_LINK_TYPE,
            {{ transform_string('CC_SERVER_TYPE') }} AS CC_SERVER_TYPE,
            {{ transform_date('GL_RATE_DATE') }} AS GL_RATE_DATE,
            {{ transform_string('TO_BE_BALANCED') }} AS TO_BE_BALANCED,
            {{ transform_string('SERVER_RESPONSE_LONG') }} AS SERVER_RESPONSE_LONG,
            {{ transform_string('EXCH_RATE_TYPE') }} AS EXCH_RATE_TYPE,
            {{ transform_date('POST_DATE') }} AS POST_DATE,
            {{ transform_numeric('BANK_ID') }} AS BANK_ID,
            {{ transform_datetime('LAST_GL_DATE') }} AS LAST_GL_DATE,
            {{ transform_numeric('AMOUNT_FULL_BASE') }} AS AMOUNT_FULL_BASE,
            {{ transform_string('USER_TYPE') }} AS USER_TYPE,
            {{ transform_numeric('USER_CLIENT_ID') }} AS USER_CLIENT_ID,
            {{ transform_numeric('USER_AGENT_ID') }} AS USER_AGENT_ID,
            {{ transform_numeric('BANK_FEE') }} AS BANK_FEE,
            {{ transform_numeric('EXCHANGE_GAIN') }} AS EXCHANGE_GAIN,
            {{ transform_string('TRANS_GUID') }} AS TRANS_GUID,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_numeric('RATE_PRICEBASE') }} AS RATE_PRICEBASE,
            {{ transform_numeric('RATE_TRANSPRICE') }} AS RATE_TRANSPRICE,
            NULL AS CURRENCY_CODE,
            NULL AS TRANS_AMOUNT,
            NULL AS TRANS_AMOUNT_LEFT,
            NULL AS AMOUNT_ORIG_CURRENCY,
            NULL AS FIRST_TRANS_ID,
            NULL AS PREV_TRANS_ID,
            NULL AS AMOUNT_ORIG_CURRENCY_LEFT,
            NULL AS EXCHANGE_RATE,
            NULL AS REF_TRANS_ID,
            NULL AS INTERN_BALANCED_COM
    FROM {{ source(var('bronze_source_prefix') ~ '_SW2', 'ACC_TRANS_DETAIL') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT *
FROM (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["TRANS_ID", "DATA_SOURCE"]) }} AS ACC_TRANS_DETAIL_SURROGATE_KEY,
        sw1_src.*
    FROM sw1_src

    UNION ALL

    SELECT
        {{ dbt_utils.generate_surrogate_key(["TRANS_ID", "DATA_SOURCE"]) }} AS ACC_TRANS_DETAIL_SURROGATE_KEY,
        sw2_src.*
    FROM sw2_src
)
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY TRANS_ID, DATA_SOURCE
        ORDER BY LAST_UPDATED_TIMESTAMP DESC
) = 1

