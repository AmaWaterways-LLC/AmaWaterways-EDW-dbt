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
        unique_key=['AGENCY_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'AGENCY') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'AGENCY') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'AGENCY', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'AGENCY') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'AGENCY', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'AGENCY') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'AGENCY') %}
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
            {{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
            {{ transform_string('AGENCY_NAME_TYPED') }} AS AGENCY_NAME_TYPED,
            {{ transform_string('AGENCY_NAME') }} AS AGENCY_NAME,
            {{ transform_string('ARC_NUMBER') }} AS ARC_NUMBER,
            {{ transform_string('IATA_NUMBER') }} AS IATA_NUMBER,
            {{ transform_string('CLIA_NUMBER') }} AS CLIA_NUMBER,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_string('IS_DOMESTIC') }} AS IS_DOMESTIC,
            {{ transform_string('IS_INTERNAL') }} AS IS_INTERNAL,
            {{ transform_string('USE_GUEST_ADDR') }} AS USE_GUEST_ADDR,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('MARKETING_MSG') }} AS MARKETING_MSG,
            {{ transform_string('IS_GSA') }} AS IS_GSA,
            {{ transform_string('CONSORTIUM_TYPE') }} AS CONSORTIUM_TYPE,
            {{ transform_string('IS_CONSORTIUM') }} AS IS_CONSORTIUM,
            {{ transform_string('AGENCY_TYPE') }} AS AGENCY_TYPE,
            {{ transform_string('DEFAULT_CURRENCY') }} AS DEFAULT_CURRENCY,
            {{ transform_string('VENDOR_NUMBER') }} AS VENDOR_NUMBER,
            {{ transform_string('USE_DFLT_CONSORTIUM') }} AS USE_DFLT_CONSORTIUM,
            {{ transform_numeric('BC_TYPE_ID') }} AS BC_TYPE_ID,
            {{ transform_string('TAXPAYER_TYPE') }} AS TAXPAYER_TYPE,
            {{ transform_string('TAXPAYER_NUMBER') }} AS TAXPAYER_NUMBER,
            {{ transform_string('CONTACT_NAME') }} AS CONTACT_NAME,
            {{ transform_string('WEB_ADDRESS') }} AS WEB_ADDRESS,
            {{ transform_string('AGENCY_NAME_ESSENTIAL') }} AS AGENCY_NAME_ESSENTIAL,
            {{ transform_string('FLOATING_DEPOSIT') }} AS FLOATING_DEPOSIT,
            {{ transform_string('LANGUAGE_CODE') }} AS LANGUAGE_CODE,
            {{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
            {{ transform_string('AG_ENTITY_TYPE') }} AS AG_ENTITY_TYPE,
            {{ transform_string('IS_FAX_EMAIL_SECURED') }} AS IS_FAX_EMAIL_SECURED,
            NULL AS ABTA_NUMBER,
            {{ transform_string('NOTIF_DFLT_DISTR_TYPE') }} AS NOTIF_DFLT_DISTR_TYPE,
            {{ transform_string('TAXOWNER_NAME') }} AS TAXOWNER_NAME,
            NULL AS INVOICING_NUMBER,
            NULL AS AGENCY_CODE,
            NULL AS FPR_AT_GROSS,
            NULL AS INVOICING_IS_STOPPED,
            NULL AS INVOICING_DELAY_UNTIL,
            NULL AS INVOICING_ERROR_MSG,
            NULL AS BALANCE_RECALC_ERROR_MSG,
            NULL AS IS_WHITELABEL,
            NULL AS VALID_CURRENCIES,
            NULL AS ALPHA_NUM_ID,
            NULL AS ALLOW_REG_COMMISS_TRANSACTIONS,
            NULL AS ALT_AGENCY_ID,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_string('COUNTRY_CODE') }} AS COUNTRY_CODE,
            {{ transform_string('GCPS_NUMBER') }} AS GCPS_NUMBER,
            {{ transform_string('KEEP_SELF_COMMISSIONS') }} AS KEEP_SELF_COMMISSIONS,
            {{ transform_string('EMAIL') }} AS EMAIL
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'AGENCY') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
            {{ transform_string('AGENCY_NAME_TYPED') }} AS AGENCY_NAME_TYPED,
            {{ transform_string('AGENCY_NAME') }} AS AGENCY_NAME,
            {{ transform_string('ARC_NUMBER') }} AS ARC_NUMBER,
            {{ transform_string('IATA_NUMBER') }} AS IATA_NUMBER,
            {{ transform_string('CLIA_NUMBER') }} AS CLIA_NUMBER,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_string('IS_DOMESTIC') }} AS IS_DOMESTIC,
            {{ transform_string('IS_INTERNAL') }} AS IS_INTERNAL,
            {{ transform_string('USE_GUEST_ADDR') }} AS USE_GUEST_ADDR,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('MARKETING_MSG') }} AS MARKETING_MSG,
            {{ transform_string('IS_GSA') }} AS IS_GSA,
            {{ transform_string('CONSORTIUM_TYPE') }} AS CONSORTIUM_TYPE,
            {{ transform_string('IS_CONSORTIUM') }} AS IS_CONSORTIUM,
            {{ transform_string('AGENCY_TYPE') }} AS AGENCY_TYPE,
            {{ transform_string('DEFAULT_CURRENCY') }} AS DEFAULT_CURRENCY,
            {{ transform_string('VENDOR_NUMBER') }} AS VENDOR_NUMBER,
            {{ transform_string('USE_DFLT_CONSORTIUM') }} AS USE_DFLT_CONSORTIUM,
            {{ transform_numeric('BC_TYPE_ID') }} AS BC_TYPE_ID,
            {{ transform_string('TAXPAYER_TYPE') }} AS TAXPAYER_TYPE,
            {{ transform_string('TAXPAYER_NUMBER') }} AS TAXPAYER_NUMBER,
            {{ transform_string('CONTACT_NAME') }} AS CONTACT_NAME,
            {{ transform_string('WEB_ADDRESS') }} AS WEB_ADDRESS,
            {{ transform_string('AGENCY_NAME_ESSENTIAL') }} AS AGENCY_NAME_ESSENTIAL,
            {{ transform_string('FLOATING_DEPOSIT') }} AS FLOATING_DEPOSIT,
            {{ transform_string('LANGUAGE_CODE') }} AS LANGUAGE_CODE,
            {{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
            {{ transform_string('AG_ENTITY_TYPE') }} AS AG_ENTITY_TYPE,
            {{ transform_string('IS_FAX_EMAIL_SECURED') }} AS IS_FAX_EMAIL_SECURED,
            {{ transform_string('ABTA_NUMBER') }} AS ABTA_NUMBER,
            {{ transform_string('NOTIF_DFLT_DISTR_TYPE') }} AS NOTIF_DFLT_DISTR_TYPE,
            {{ transform_string('TAXOWNER_NAME') }} AS TAXOWNER_NAME,
            {{ transform_numeric('INVOICING_NUMBER') }} AS INVOICING_NUMBER,
            {{ transform_string('AGENCY_CODE') }} AS AGENCY_CODE,
            {{ transform_string('FPR_AT_GROSS') }} AS FPR_AT_GROSS,
            {{ transform_string('INVOICING_IS_STOPPED') }} AS INVOICING_IS_STOPPED,
            {{ transform_date('INVOICING_DELAY_UNTIL') }} AS INVOICING_DELAY_UNTIL,
            {{ transform_string('INVOICING_ERROR_MSG') }} AS INVOICING_ERROR_MSG,
            {{ transform_string('BALANCE_RECALC_ERROR_MSG') }} AS BALANCE_RECALC_ERROR_MSG,
            {{ transform_string('IS_WHITELABEL') }} AS IS_WHITELABEL,
            {{ transform_string('VALID_CURRENCIES') }} AS VALID_CURRENCIES,
            {{ transform_string('ALPHA_NUM_ID') }} AS ALPHA_NUM_ID,
            {{ transform_string('ALLOW_REG_COMMISS_TRANSACTIONS') }} AS ALLOW_REG_COMMISS_TRANSACTIONS,
            {{ transform_string('ALT_AGENCY_ID') }} AS ALT_AGENCY_ID,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            NULL AS COUNTRY_CODE,
            NULL AS GCPS_NUMBER,
            NULL AS KEEP_SELF_COMMISSIONS,
            NULL AS EMAIL
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'AGENCY') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["AGENCY_ID", "DATA_SOURCE"]) }} AS AGENCY_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["AGENCY_ID", "DATA_SOURCE"]) }} AS AGENCY_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

