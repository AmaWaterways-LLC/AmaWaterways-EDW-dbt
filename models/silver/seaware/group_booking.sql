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
        unique_key=['_FIVETRAN_ID', 'GROUP_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'GROUP_BOOKING') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_group_booking',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='GROUP_BOOKING',
                     layer='SILVER',
                     operation_type='MERGE',
                     load_type=load_type_val,
                     environment=target.name,
                     ingested_by='dbt',
                     batch_id='" ~ batch_id ~ "'
                 ) %}              
             {% endif %}"
        ],
        post_hook=[
            "{% do audit_update_counts(batch_id='" ~ batch_id ~ "', record_count_target=get_record_count(this)) %}",
            "{% do audit_end(batch_id='" ~ batch_id ~ "', status='SUCCESS') %}",
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'GROUP_BOOKING') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'GROUP_BOOKING', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'GROUP_BOOKING') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'GROUP_BOOKING', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'GROUP_BOOKING') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'GROUP_BOOKING') %}
    {% set wm_col_sw2 = cfg_sw2['WATERMARK_COLUMN'] %}
    {% set last_wm_sw2 = cfg_sw2['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw2 = (last_wm is none) %}
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

with sw1_src as (
    select
    'SW1' AS DATA_SOURCE,
{{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
{{ transform_numeric('AGENT_ID') }} AS AGENT_ID,
{{ transform_string('GROUP_NAME') }} AS GROUP_NAME,
{{ transform_string('GROUP_TYPE') }} AS GROUP_TYPE,
{{ transform_numeric('N_OF_GUESTS') }} AS N_OF_GUESTS,
{{ transform_numeric('REL_DAY_FROM') }} AS REL_DAY_FROM,
{{ transform_numeric('REL_DAY_TO') }} AS REL_DAY_TO,
{{ transform_datetime('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
{{ transform_datetime('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
{{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
{{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
{{ transform_string('IS_VALID') }} AS IS_VALID,
{{ transform_numeric('PROBABILITY') }} AS PROBABILITY,
{{ transform_string('GROUP_STATUS') }} AS GROUP_STATUS,
{{ transform_string('ALLOW_DIFF_AGENCY') }} AS ALLOW_DIFF_AGENCY,
{{ transform_numeric('EXPIRATION_EXTENSION') }} AS EXPIRATION_EXTENSION,
{{ transform_datetime('GROUP_INIT_DATE') }} AS GROUP_INIT_DATE,
{{ transform_string('WDW_GROUP_ID') }} AS WDW_GROUP_ID,
{{ transform_numeric('PRIORITY') }} AS PRIORITY,
{{ transform_string('TERMINATION_COMPLETED') }} AS TERMINATION_COMPLETED,
{{ transform_numeric('GROUP_SIZE_OVERRIDE') }} AS GROUP_SIZE_OVERRIDE,
{{ transform_numeric('PROBABILITY_ALLOCATED') }} AS PROBABILITY_ALLOCATED,
{{ transform_string('IS_CONTRACT_RATE') }} AS IS_CONTRACT_RATE,
{{ transform_string('CAN_PROCESS_GTB') }} AS CAN_PROCESS_GTB,
{{ transform_string('GRP_COORD_NAME') }} AS GRP_COORD_NAME,
{{ transform_string('USE_TC_FULLPAY') }} AS USE_TC_FULLPAY,
{{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
{{ transform_numeric('SEC_AGENCY_ID') }} AS SEC_AGENCY_ID,
{{ transform_string('LANGUAGE_CODE') }} AS LANGUAGE_CODE,
{{ transform_string('IS_INVOICE_VALID') }} AS IS_INVOICE_VALID,
{{ transform_string('IS_CHARTER') }} AS IS_CHARTER,
{{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
{{ transform_string('IS_PROB_MANUAL') }} AS IS_PROB_MANUAL,
{{ transform_string('ALLOW_UNBALANCE') }} AS ALLOW_UNBALANCE,
{{ transform_numeric('CURRENCY_RATE') }} AS CURRENCY_RATE,
{{ transform_string('IS_UNBALANCED') }} AS IS_UNBALANCED,
{{ transform_string('GB_ENTITY_TYPE') }} AS GB_ENTITY_TYPE,
{{ transform_numeric('INVOICE_VERSION') }} AS INVOICE_VERSION,
{{ transform_datetime('FIRST_FULL_PAYMENT_PROCESSED') }} AS FIRST_FULL_PAYMENT_PROCESSED,
{{ transform_string('ALLOW_AUTO_PROCESS_FPR') }} AS ALLOW_AUTO_PROCESS_FPR,
{{ transform_string('DEFAULT_INSURANCE') }} AS DEFAULT_INSURANCE,
{{ transform_numeric('AM_ORDER_ID') }} AS AM_ORDER_ID,
{{ transform_string('FPR_IN_PROCESS') }} AS FPR_IN_PROCESS,
{{ transform_numeric('COMPANY_ID') }} AS COMPANY_ID,
{{ transform_numeric('UPDATE_VERSION') }} AS UPDATE_VERSION,
NULL AS DEP_REF_ID,
NULL AS ARR_REF_ID,
NULL AS IS_PAYSCH_MANUAL,
{{ transform_numeric('SEC_AGENT_ID') }} AS SEC_AGENT_ID,
NULL AS COMPANY_AGENT_ID,
NULL AS CLIENT_ID,
NULL AS VT_PACKAGE_ID,
NULL AS OWNERSHIP_CODE,
NULL AS INVOICE_GEN_DEFERRED,
NULL AS VAC_DATE_FROM,
NULL AS VAC_DATE_TO,
NULL AS TERMS_CONFIRMED,
NULL AS SOURCE_CODE,
NULL AS GROUP_MODE,
NULL AS ALLOW_INVOICING,
NULL AS INVENTORY_RELEASE_DATE,
NULL AS USE_FIT_PAYSCH,
NULL AS TC_SAIL_ID,
NULL AS PROCESS_AS_WHOLE_ENTITY,
NULL AS AUTO_DISTRIBUTE_MONEY,
NULL AS EXTRA_AGENCY_ID,
NULL AS TOUR_PACKAGE_ID,
NULL AS ALLOW_DIFF_CURR_RATE,
NULL AS DISTR_MONEY_TYPE,
NULL AS GROUP_GUID,
NULL AS QUOTE_CONFIRMATION_DATE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
{{ transform_string('ALLOW_AUTO_DISTR_FUNDS') }} AS ALLOW_AUTO_DISTR_FUNDS,
{{ transform_string('ALLOW_AUTO_CANCEL_DLGT') }} AS ALLOW_AUTO_CANCEL_DLGT,
{{ transform_numeric('TOUR_CREDIT_POLICY_OVR') }} AS TOUR_CREDIT_POLICY_OVR,
{{ transform_string('ALLOW_REFUND_CLIENT_MONEY') }} AS ALLOW_REFUND_CLIENT_MONEY
    from {{ source('AMA_PROD_BRNZ_SW1', 'GROUP_BOOKING') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
{{ transform_string('COMMENTS') }} AS COMMENTS,
{{ transform_numeric('AGENCY_ID') }} AS AGENCY_ID,
{{ transform_numeric('AGENT_ID') }} AS AGENT_ID,
{{ transform_string('GROUP_NAME') }} AS GROUP_NAME,
{{ transform_string('GROUP_TYPE') }} AS GROUP_TYPE,
{{ transform_numeric('N_OF_GUESTS') }} AS N_OF_GUESTS,
{{ transform_numeric('REL_DAY_FROM') }} AS REL_DAY_FROM,
{{ transform_numeric('REL_DAY_TO') }} AS REL_DAY_TO,
{{ transform_datetime('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
{{ transform_datetime('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
{{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
{{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
{{ transform_string('IS_VALID') }} AS IS_VALID,
{{ transform_numeric('PROBABILITY') }} AS PROBABILITY,
{{ transform_string('GROUP_STATUS') }} AS GROUP_STATUS,
{{ transform_string('ALLOW_DIFF_AGENCY') }} AS ALLOW_DIFF_AGENCY,
{{ transform_numeric('EXPIRATION_EXTENSION') }} AS EXPIRATION_EXTENSION,
{{ transform_datetime('GROUP_INIT_DATE') }} AS GROUP_INIT_DATE,
{{ transform_string('WDW_GROUP_ID') }} AS WDW_GROUP_ID,
{{ transform_numeric('PRIORITY') }} AS PRIORITY,
{{ transform_string('TERMINATION_COMPLETED') }} AS TERMINATION_COMPLETED,
{{ transform_numeric('GROUP_SIZE_OVERRIDE') }} AS GROUP_SIZE_OVERRIDE,
{{ transform_numeric('PROBABILITY_ALLOCATED') }} AS PROBABILITY_ALLOCATED,
{{ transform_string('IS_CONTRACT_RATE') }} AS IS_CONTRACT_RATE,
{{ transform_string('CAN_PROCESS_GTB') }} AS CAN_PROCESS_GTB,
{{ transform_string('GRP_COORD_NAME') }} AS GRP_COORD_NAME,
{{ transform_string('USE_TC_FULLPAY') }} AS USE_TC_FULLPAY,
{{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
{{ transform_numeric('SEC_AGENCY_ID') }} AS SEC_AGENCY_ID,
{{ transform_string('LANGUAGE_CODE') }} AS LANGUAGE_CODE,
{{ transform_string('IS_INVOICE_VALID') }} AS IS_INVOICE_VALID,
{{ transform_string('IS_CHARTER') }} AS IS_CHARTER,
{{ transform_string('OFFICE_CODE') }} AS OFFICE_CODE,
{{ transform_string('IS_PROB_MANUAL') }} AS IS_PROB_MANUAL,
{{ transform_string('ALLOW_UNBALANCE') }} AS ALLOW_UNBALANCE,
{{ transform_numeric('CURRENCY_RATE') }} AS CURRENCY_RATE,
{{ transform_string('IS_UNBALANCED') }} AS IS_UNBALANCED,
{{ transform_string('GB_ENTITY_TYPE') }} AS GB_ENTITY_TYPE,
{{ transform_numeric('INVOICE_VERSION') }} AS INVOICE_VERSION,
{{ transform_datetime('FIRST_FULL_PAYMENT_PROCESSED') }} AS FIRST_FULL_PAYMENT_PROCESSED,
{{ transform_string('ALLOW_AUTO_PROCESS_FPR') }} AS ALLOW_AUTO_PROCESS_FPR,
{{ transform_string('DEFAULT_INSURANCE') }} AS DEFAULT_INSURANCE,
{{ transform_numeric('AM_ORDER_ID') }} AS AM_ORDER_ID,
{{ transform_string('FPR_IN_PROCESS') }} AS FPR_IN_PROCESS,
{{ transform_numeric('COMPANY_ID') }} AS COMPANY_ID,
{{ transform_numeric('UPDATE_VERSION') }} AS UPDATE_VERSION,
{{ transform_numeric('DEP_REF_ID') }} AS DEP_REF_ID,
{{ transform_numeric('ARR_REF_ID') }} AS ARR_REF_ID,
{{ transform_string('IS_PAYSCH_MANUAL') }} AS IS_PAYSCH_MANUAL,
{{ transform_numeric('SEC_AGENT_ID') }} AS SEC_AGENT_ID,
{{ transform_numeric('COMPANY_AGENT_ID') }} AS COMPANY_AGENT_ID,
{{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
{{ transform_numeric('VT_PACKAGE_ID') }} AS VT_PACKAGE_ID,
{{ transform_string('OWNERSHIP_CODE') }} AS OWNERSHIP_CODE,
{{ transform_string('INVOICE_GEN_DEFERRED') }} AS INVOICE_GEN_DEFERRED,
{{ transform_datetime('VAC_DATE_FROM') }} AS VAC_DATE_FROM,
{{ transform_datetime('VAC_DATE_TO') }} AS VAC_DATE_TO,
{{ transform_string('TERMS_CONFIRMED') }} AS TERMS_CONFIRMED,
{{ transform_string('SOURCE_CODE') }} AS SOURCE_CODE,
{{ transform_string('GROUP_MODE') }} AS GROUP_MODE,
{{ transform_string('ALLOW_INVOICING') }} AS ALLOW_INVOICING,
{{ transform_datetime('INVENTORY_RELEASE_DATE') }} AS INVENTORY_RELEASE_DATE,
{{ transform_string('USE_FIT_PAYSCH') }} AS USE_FIT_PAYSCH,
{{ transform_numeric('TC_SAIL_ID') }} AS TC_SAIL_ID,
{{ transform_string('PROCESS_AS_WHOLE_ENTITY') }} AS PROCESS_AS_WHOLE_ENTITY,
{{ transform_string('AUTO_DISTRIBUTE_MONEY') }} AS AUTO_DISTRIBUTE_MONEY,
{{ transform_numeric('EXTRA_AGENCY_ID') }} AS EXTRA_AGENCY_ID,
{{ transform_numeric('TOUR_PACKAGE_ID') }} AS TOUR_PACKAGE_ID,
{{ transform_string('ALLOW_DIFF_CURR_RATE') }} AS ALLOW_DIFF_CURR_RATE,
{{ transform_string('DISTR_MONEY_TYPE') }} AS DISTR_MONEY_TYPE,
{{ transform_string('GROUP_GUID') }} AS GROUP_GUID,
{{ transform_datetime('QUOTE_CONFIRMATION_DATE') }} AS QUOTE_CONFIRMATION_DATE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
NULL AS ALLOW_AUTO_DISTR_FUNDS,
NULL AS ALLOW_AUTO_CANCEL_DLGT,
NULL AS TOUR_CREDIT_POLICY_OVR,
NULL AS ALLOW_REFUND_CLIENT_MONEY
    from {{ source('AMA_PROD_BRNZ_SW2', 'GROUP_BOOKING') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
