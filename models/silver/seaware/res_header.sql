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
        unique_key=['RES_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'RES_HEADER') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'RES_HEADER') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'RES_HEADER', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'RES_HEADER') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'RES_HEADER', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'RES_HEADER') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'RES_HEADER') %}
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
            {{ transform_numeric('RES_ID') }} AS RES_ID,
            {{ transform_datetime('RES_INIT_DATE') }} AS RES_INIT_DATE,
            {{ transform_numeric('RES_GUEST_COUNT') }} AS RES_GUEST_COUNT,
            {{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
            {{ transform_numeric('AGENT_ID') }} AS AGENT_ID,
            {{ transform_string('CABIN_CATEGORY') }} AS CABIN_CATEGORY,
            {{ transform_string('RES_STATUS') }} AS RES_STATUS,
            {{ transform_numeric('PROBABILITY') }} AS PROBABILITY,
            {{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
            {{ transform_numeric('DELEGATE_HEADER_ID') }} AS DELEGATE_HEADER_ID,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('CRS_STATUS') }} AS CRS_STATUS,
            {{ transform_string('DPMS_STATUS') }} AS DPMS_STATUS,
            {{ transform_string('TERMINATION_COMPLETED') }} AS TERMINATION_COMPLETED,
            {{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
            {{ transform_string('PACKAGE_TYPE') }} AS PACKAGE_TYPE,
            {{ transform_date('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
            {{ transform_date('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
            {{ transform_numeric('REL_DAY_FROM') }} AS REL_DAY_FROM,
            {{ transform_numeric('REL_DAY_TO') }} AS REL_DAY_TO,
            {{ transform_string('SOURCE_CODE') }} AS SOURCE_CODE,
            {{ transform_string('ALT_RES_ID') }} AS ALT_RES_ID,
            {{ transform_numeric('SEC_AGENCY_ID') }} AS SEC_AGENCY_ID,
            {{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
            {{ transform_numeric('CURRENCY_RATE') }} AS CURRENCY_RATE,
            {{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
            {{ transform_string('RH_ENTITY_TYPE') }} AS RH_ENTITY_TYPE,
            NULL AS CANCELLATION_CASE,
            NULL AS OFFICE_LOCATION,
            NULL AS DEP_REF_ID,
            NULL AS ARR_REF_ID,
            NULL AS SEC_AGENT_ID,
            NULL AS COMPANY_ID,
            NULL AS COMPANY_AGENT_ID,
            NULL AS CONTACT_ID,
            NULL AS RES_TYPE,
            NULL AS PARENT_RES_ID,
            NULL AS OWNERSHIP_CODE,
            NULL AS VT_PACKAGE_ID,
            NULL AS RES_NAME,
            NULL AS LAST_UPDATED_AT,
            NULL AS IS_PROB_MANUAL,
            NULL AS RES_MODE,
            NULL AS CUSTOMER_REFERENCE,
            NULL AS AGREEMENT_AGENCY_ID,
            NULL AS WAS_IMO_CONTROLLED,
            NULL AS CONSIGNEE_ID,
            NULL AS CONSIGNEE,
            NULL AS ALLOTMENT_HEADER_ID,
            NULL AS NEEDS_CONTROL_BEFORE_INVOICE,
            NULL AS CONSIGNEE_CLIENT_ID,
            NULL AS CARGO_DEV_TYPE,
            NULL AS CARGO_DEV_TYPE_EXT,
            NULL AS CARGO_DEV_INFO,
            NULL AS CARGO_DEV_STATUS,
            NULL AS SELLER_ID,
            NULL AS SELLER_VAT_ID,
            NULL AS BUYER_VAT_ID,
            NULL AS ALLOW_INVOICING,
            NULL AS BUYER_ID,
            NULL AS LANGUAGE_CODE,
            NULL AS LAST_PAYMENT_EXT_DATE,
            NULL AS CAB_DISTRIB_BYPASS_ELIGIB_RULE,
            {{ transform_string('ALT_GROUPING') }} AS ALT_GROUPING,
            NULL AS HAS_EXTERNAL_COMPONENTS,
            NULL AS BOARDING_ZONE,
            NULL AS SHIPPER_ID,
            NULL AS CONSIDER_PAID,
            NULL AS CARGO_BY_SHIPPER_ORDER,
            NULL AS NOTIFY_AGENCY_ID,
            NULL AS NOTIFY_CLIENT_ID,
            NULL AS NOTIFY_MESSAGE,
            NULL AS CARGO_BY_CONSIGNEE_ORDER,
            NULL AS EXT_REQUESTER_CLIENT_ID,
            NULL AS EXT_REQUESTER_AGENCY_ID,
            NULL AS EXTRA_AGENCY_ID,
            NULL AS HOLD_UPDATES_TO_AIR_SYSTEM,
            NULL AS RES_VERSION,
            NULL AS OFFER_CODE,
            NULL AS TRW_REFERRAL_CODE,
            NULL AS PAYER_CLIENT_ID,
            NULL AS ADDRESS_GUEST_AS,
            {{ transform_string('ORIGIN_COUNTRY_CODE') }} AS ORIGIN_COUNTRY_CODE,
            NULL AS EXT_REQUESTER_AGENT_ID,
            NULL AS GIFT_CARD_TO,
            NULL AS GIFT_CARD_MSG,
            NULL AS GIFT_CARD_COMPLIMENTS_OF,
            NULL AS ALPHA_NUM_ID,
            NULL AS ORIGINAL_INIT_DATE,
            NULL AS GROUP_AUTO_DISTR_DISABLED,
            NULL AS TERMS_ENABLED,
            NULL AS TERMS_ENABLED_OVR,
            NULL AS FPR_AT_GROSS,
            NULL AS CONFIRMATION_DATE,
            NULL AS QUOTE_CONFIRMATION_DATE,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_numeric('AM_ORDER_ID') }} AS AM_ORDER_ID,
            {{ transform_numeric('COMMISSION_AGENCY_ID') }} AS COMMISSION_AGENCY_ID,
            {{ transform_numeric('COLLECTION_ID') }} AS COLLECTION_ID,
            {{ transform_numeric('ESEAAIR_VERSION_SW') }} AS ESEAAIR_VERSION_SW,
            {{ transform_numeric('ESEAAIR_VERSION_AW') }} AS ESEAAIR_VERSION_AW
    FROM {{ source(var('bronze_source_prefix') ~ '_SW1', 'RES_HEADER') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('RES_ID') }} AS RES_ID,
            {{ transform_datetime('RES_INIT_DATE') }} AS RES_INIT_DATE,
            {{ transform_numeric('RES_GUEST_COUNT') }} AS RES_GUEST_COUNT,
            {{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
            {{ transform_numeric('AGENT_ID') }} AS AGENT_ID,
            {{ transform_string('CABIN_CATEGORY') }} AS CABIN_CATEGORY,
            {{ transform_string('RES_STATUS') }} AS RES_STATUS,
            {{ transform_numeric('PROBABILITY') }} AS PROBABILITY,
            {{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
            {{ transform_numeric('DELEGATE_HEADER_ID') }} AS DELEGATE_HEADER_ID,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('CRS_STATUS') }} AS CRS_STATUS,
            {{ transform_string('DPMS_STATUS') }} AS DPMS_STATUS,
            {{ transform_string('TERMINATION_COMPLETED') }} AS TERMINATION_COMPLETED,
            {{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
            {{ transform_string('PACKAGE_TYPE') }} AS PACKAGE_TYPE,
            {{ transform_date('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
            {{ transform_date('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
            {{ transform_numeric('REL_DAY_FROM') }} AS REL_DAY_FROM,
            {{ transform_numeric('REL_DAY_TO') }} AS REL_DAY_TO,
            {{ transform_string('SOURCE_CODE') }} AS SOURCE_CODE,
            {{ transform_string('ALT_RES_ID') }} AS ALT_RES_ID,
            {{ transform_numeric('SEC_AGENCY_ID') }} AS SEC_AGENCY_ID,
            {{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
            {{ transform_numeric('CURRENCY_RATE') }} AS CURRENCY_RATE,
            {{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
            {{ transform_string('RH_ENTITY_TYPE') }} AS RH_ENTITY_TYPE,
            {{ transform_numeric('CANCELLATION_CASE') }} AS CANCELLATION_CASE,
            {{ transform_string('OFFICE_LOCATION') }} AS OFFICE_LOCATION,
            {{ transform_numeric('DEP_REF_ID') }} AS DEP_REF_ID,
            {{ transform_numeric('ARR_REF_ID') }} AS ARR_REF_ID,
            {{ transform_numeric('SEC_AGENT_ID') }} AS SEC_AGENT_ID,
            {{ transform_numeric('COMPANY_ID') }} AS COMPANY_ID,
            {{ transform_numeric('COMPANY_AGENT_ID') }} AS COMPANY_AGENT_ID,
            {{ transform_numeric('CONTACT_ID') }} AS CONTACT_ID,
            {{ transform_string('RES_TYPE') }} AS RES_TYPE,
            {{ transform_numeric('PARENT_RES_ID') }} AS PARENT_RES_ID,
            {{ transform_string('OWNERSHIP_CODE') }} AS OWNERSHIP_CODE,
            {{ transform_numeric('VT_PACKAGE_ID') }} AS VT_PACKAGE_ID,
            {{ transform_string('RES_NAME') }} AS RES_NAME,
            {{ transform_datetime('LAST_UPDATED_AT') }} AS LAST_UPDATED_AT,
            {{ transform_string('IS_PROB_MANUAL') }} AS IS_PROB_MANUAL,
            {{ transform_string('RES_MODE') }} AS RES_MODE,
            {{ transform_string('CUSTOMER_REFERENCE') }} AS CUSTOMER_REFERENCE,
            {{ transform_numeric('AGREEMENT_AGENCY_ID') }} AS AGREEMENT_AGENCY_ID,
            {{ transform_string('WAS_IMO_CONTROLLED') }} AS WAS_IMO_CONTROLLED,
            {{ transform_numeric('CONSIGNEE_ID') }} AS CONSIGNEE_ID,
            {{ transform_string('CONSIGNEE') }} AS CONSIGNEE,
            {{ transform_numeric('ALLOTMENT_HEADER_ID') }} AS ALLOTMENT_HEADER_ID,
            {{ transform_string('NEEDS_CONTROL_BEFORE_INVOICE') }} AS NEEDS_CONTROL_BEFORE_INVOICE,
            {{ transform_numeric('CONSIGNEE_CLIENT_ID') }} AS CONSIGNEE_CLIENT_ID,
            {{ transform_string('CARGO_DEV_TYPE') }} AS CARGO_DEV_TYPE,
            {{ transform_string('CARGO_DEV_TYPE_EXT') }} AS CARGO_DEV_TYPE_EXT,
            {{ transform_string('CARGO_DEV_INFO') }} AS CARGO_DEV_INFO,
            {{ transform_string('CARGO_DEV_STATUS') }} AS CARGO_DEV_STATUS,
            {{ transform_numeric('SELLER_ID') }} AS SELLER_ID,
            {{ transform_numeric('SELLER_VAT_ID') }} AS SELLER_VAT_ID,
            {{ transform_numeric('BUYER_VAT_ID') }} AS BUYER_VAT_ID,
            {{ transform_string('ALLOW_INVOICING') }} AS ALLOW_INVOICING,
            {{ transform_numeric('BUYER_ID') }} AS BUYER_ID,
            {{ transform_string('LANGUAGE_CODE') }} AS LANGUAGE_CODE,
            {{ transform_date('LAST_PAYMENT_EXT_DATE') }} AS LAST_PAYMENT_EXT_DATE,
            {{ transform_string('CAB_DISTRIB_BYPASS_ELIGIB_RULE') }} AS CAB_DISTRIB_BYPASS_ELIGIB_RULE,
            {{ transform_string('ALT_GROUPING') }} AS ALT_GROUPING,
            {{ transform_string('HAS_EXTERNAL_COMPONENTS') }} AS HAS_EXTERNAL_COMPONENTS,
            {{ transform_string('BOARDING_ZONE') }} AS BOARDING_ZONE,
            {{ transform_numeric('SHIPPER_ID') }} AS SHIPPER_ID,
            {{ transform_string('CONSIDER_PAID') }} AS CONSIDER_PAID,
            {{ transform_string('CARGO_BY_SHIPPER_ORDER') }} AS CARGO_BY_SHIPPER_ORDER,
            {{ transform_numeric('NOTIFY_AGENCY_ID') }} AS NOTIFY_AGENCY_ID,
            {{ transform_numeric('NOTIFY_CLIENT_ID') }} AS NOTIFY_CLIENT_ID,
            {{ transform_string('NOTIFY_MESSAGE') }} AS NOTIFY_MESSAGE,
            {{ transform_string('CARGO_BY_CONSIGNEE_ORDER') }} AS CARGO_BY_CONSIGNEE_ORDER,
            {{ transform_numeric('EXT_REQUESTER_CLIENT_ID') }} AS EXT_REQUESTER_CLIENT_ID,
            {{ transform_numeric('EXT_REQUESTER_AGENCY_ID') }} AS EXT_REQUESTER_AGENCY_ID,
            {{ transform_numeric('EXTRA_AGENCY_ID') }} AS EXTRA_AGENCY_ID,
            {{ transform_string('HOLD_UPDATES_TO_AIR_SYSTEM') }} AS HOLD_UPDATES_TO_AIR_SYSTEM,
            {{ transform_numeric('RES_VERSION') }} AS RES_VERSION,
            {{ transform_string('OFFER_CODE') }} AS OFFER_CODE,
            {{ transform_string('TRW_REFERRAL_CODE') }} AS TRW_REFERRAL_CODE,
            {{ transform_numeric('PAYER_CLIENT_ID') }} AS PAYER_CLIENT_ID,
            {{ transform_string('ADDRESS_GUEST_AS') }} AS ADDRESS_GUEST_AS,
            {{ transform_string('ORIGIN_COUNTRY_CODE') }} AS ORIGIN_COUNTRY_CODE,
            {{ transform_numeric('EXT_REQUESTER_AGENT_ID') }} AS EXT_REQUESTER_AGENT_ID,
            {{ transform_string('GIFT_CARD_TO') }} AS GIFT_CARD_TO,
            {{ transform_string('GIFT_CARD_MSG') }} AS GIFT_CARD_MSG,
            {{ transform_string('GIFT_CARD_COMPLIMENTS_OF') }} AS GIFT_CARD_COMPLIMENTS_OF,
            {{ transform_string('ALPHA_NUM_ID') }} AS ALPHA_NUM_ID,
            {{ transform_datetime('ORIGINAL_INIT_DATE') }} AS ORIGINAL_INIT_DATE,
            {{ transform_string('GROUP_AUTO_DISTR_DISABLED') }} AS GROUP_AUTO_DISTR_DISABLED,
            {{ transform_string('TERMS_ENABLED') }} AS TERMS_ENABLED,
            {{ transform_string('TERMS_ENABLED_OVR') }} AS TERMS_ENABLED_OVR,
            {{ transform_string('FPR_AT_GROSS') }} AS FPR_AT_GROSS,
            {{ transform_datetime('CONFIRMATION_DATE') }} AS CONFIRMATION_DATE,
            {{ transform_datetime('QUOTE_CONFIRMATION_DATE') }} AS QUOTE_CONFIRMATION_DATE,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            NULL AS AM_ORDER_ID,
            NULL AS COMMISSION_AGENCY_ID,
            NULL AS COLLECTION_ID,
            NULL AS ESEAAIR_VERSION_SW,
            NULL AS ESEAAIR_VERSION_AW
    FROM {{ source(var('bronze_source_prefix') ~ '_SW2', 'RES_HEADER') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT *
FROM (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["RES_ID", "DATA_SOURCE"]) }} AS RES_HEADER_SURROGATE_KEY,
        sw1_src.*
    FROM sw1_src

    UNION ALL

    SELECT
        {{ dbt_utils.generate_surrogate_key(["RES_ID", "DATA_SOURCE"]) }} AS RES_HEADER_SURROGATE_KEY,
        sw2_src.*
    FROM sw2_src
)
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY RES_ID, DATA_SOURCE
        ORDER BY LAST_UPDATED_TIMESTAMP DESC
) = 1

