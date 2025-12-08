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
        unique_key='id',
        on_schema_change="sync_all_columns",
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SALESFORCE', target.database, target.schema, 'OPPORTUNITY') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_opportunity',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='OPPORTUNITY',
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
                 {% set wm_col = get_watermark_column('SALESFORCE', target.database, target.schema, 'OPPORTUNITY') %}
                 {% set max_wm = compute_max_watermark(this, wm_col) %}
                 {% if max_wm is not none %}
                     {% do update_config_watermark('SALESFORCE', target.database, target.schema, 'OPPORTUNITY', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if var('config_lookup_debug', false) %}
    {% do log("Executing SQL for get_config_row: Step 1", info=True) %}
{% endif %}

{% if execute %}
    {% set cfg = get_config_row('SALESFORCE', target.database, target.schema, 'OPPORTUNITY') %}
    {% set wm_col = cfg['WATERMARK_COLUMN'] %}
    {% set last_wm = cfg['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full = (last_wm is none) %}
{% else %}
    {% set wm_col = none %}
    {% set last_wm = none %}
    {% set is_full = true %}
{% endif %}

{# ================================================================
   SOURCE CTE
   ================================================================ #}
{% if var('config_lookup_debug', false) %}
    {% do log("Executing SQL for get_config_row: Step 2", info=True) %}
{% endif %}

with src as (
    select
    {{ String_Transform('ACCOUNT_ID') }} as ACCOUNT_ID,
    {{ Boolean_Transform('ADDED_TC_TO_GROUP_C') }} as ADDED_TC_TO_GROUP_C,
    {{ DateTime_Transform('ADD_AIR_SHELL_TRANSFER_REQUESTS_C') }} as ADD_AIR_SHELL_TRANSFER_REQUESTS_C,
    {{ String_Transform('ADD_AIR_SHELL_TRANSFER_REQUESTS_COMMENT_C') }} as ADD_AIR_SHELL_TRANSFER_REQUESTS_COMMENT_C,
    {{ DateTime_Transform('ADD_SPECIAL_REQUESTS_ON_BOOKINGS_C') }} as ADD_SPECIAL_REQUESTS_ON_BOOKINGS_C,
    {{ String_Transform('ADD_SPECIAL_REQUESTS_ON_BOOKINGS_COMMENT_C') }} as ADD_SPECIAL_REQUESTS_ON_BOOKINGS_COMMENT_C,
    {{ DateTime_Transform('ADVISE_90_DAYS_FINAL_PAYMENT_DUE_C') }} as ADVISE_90_DAYS_FINAL_PAYMENT_DUE_C,
    {{ String_Transform('ADVISE_90_DAYS_FINAL_PAYMENT_DUE_COMMENT_C') }} as ADVISE_90_DAYS_FINAL_PAYMENT_DUE_COMMENT_C,
    {{ Boolean_Transform('ADVISE_TC_TO_GROUP_C') }} as ADVISE_TC_TO_GROUP_C,
    {{ String_Transform('ADVISE_TC_TO_GROUP_COMMENT_C') }} as ADVISE_TC_TO_GROUP_COMMENT_C,
    {{ String_Transform('AGENT_ID_C') }} as AGENT_ID_C,
    {{ Number_Transform('AMOUNT') }} as AMOUNT,
    {{ DateTime_Transform('ASSIGN_A_GROUP_COORDINATOR_C') }} as ASSIGN_A_GROUP_COORDINATOR_C,
    {{ DateTime_Transform('ASSIST_TA_WITH_FLYER_REQUEST_C') }} as ASSIST_TA_WITH_FLYER_REQUEST_C,
    {{ String_Transform('ASSIST_TA_WITH_FLYER_REQUEST_COMMENT_C') }} as ASSIST_TA_WITH_FLYER_REQUEST_COMMENT_C,
    {{ Boolean_Transform('BOOKED_WINE_HOST_CABIN_C') }} as BOOKED_WINE_HOST_CABIN_C,
    {{ String_Transform('BOOKED_WINE_HOST_CABIN_COMMENT_C') }} as BOOKED_WINE_HOST_CABIN_COMMENT_C,
    {{ Boolean_Transform('BOOK_WINE_HOST_AIR_C') }} as BOOK_WINE_HOST_AIR_C,
    {{ String_Transform('BOOK_WINE_HOST_AIR_COMMENT_C') }} as BOOK_WINE_HOST_AIR_COMMENT_C,
    {{ String_Transform('BUYER_C') }} as BUYER_C,
    {{ String_Transform('CAMPAIGN_ID') }} as CAMPAIGN_ID,
    {{ DateTime_Transform('CHARTER_PROPOSAL_CREATE_DATE_C') }} as CHARTER_PROPOSAL_CREATE_DATE_C,
    {{ String_Transform('CHARTER_REFERRAL_BY_C') }} as CHARTER_REFERRAL_BY_C,
    {{ String_Transform('CHARTER_SALES_COORDINATOR_C') }} as CHARTER_SALES_COORDINATOR_C,
    {{ Boolean_Transform('CHEF_S_TABLE_REQUEST_SUBMITTED_C') }} as CHEF_S_TABLE_REQUEST_SUBMITTED_C,
    {{ String_Transform('CHEF_S_TABLE_REQUEST_SUBMITTED_COMMENT_C') }} as CHEF_S_TABLE_REQUEST_SUBMITTED_COMMENT_C,
    {{ DateTime_Transform('CLOSE_DATE') }} as CLOSE_DATE,
    {{ String_Transform('CM_REQUEST_C') }} as CM_REQUEST_C,
    {{ Boolean_Transform('COLLECT_PASSPORT_INFORMATION_DATE_C') }} as COLLECT_PASSPORT_INFORMATION_DATE_C,
    {{ String_Transform('COMMENTS_C') }} as COMMENTS_C,
    {{ DateTime_Transform('COMPLETE_GROUP_SUMMARY_C') }} as COMPLETE_GROUP_SUMMARY_C,
    {{ String_Transform('COMPLETE_GROUP_SUMMARY_COMMENT_C') }} as COMPLETE_GROUP_SUMMARY_COMMENT_C,
    {{ Boolean_Transform('CONFIRM_OTHER_SPECIAL_REQUESTS_C') }} as CONFIRM_OTHER_SPECIAL_REQUESTS_C,
    {{ String_Transform('CONTACT_EMAIL_C') }} as CONTACT_EMAIL_C,
    {{ String_Transform('CONTACT_ID') }} as CONTACT_ID,
    {{ String_Transform('CONTACT_PHONE_C') }} as CONTACT_PHONE_C,
    {{ String_Transform('CREATED_BY_ID') }} as CREATED_BY_ID,
    {{ DateTime_Transform('CREATED_DATE') }} as CREATED_DATE,
    {{ DateTime_Transform('DEPOSIT_DATE_C') }} as DEPOSIT_DATE_C,
    {{ String_Transform('DESCRIPTION') }} as DESCRIPTION,
    {{ DateTime_Transform('DISEMBARKATION_DATE_C') }} as DISEMBARKATION_DATE_C,
    {{ DateTime_Transform('DOC_SENT_TRACKING_NUMBER_SENT_C') }} as DOC_SENT_TRACKING_NUMBER_SENT_C,
    {{ String_Transform('DOC_SENT_TRACKING_NUMBER_SENT_COMMENT_C') }} as DOC_SENT_TRACKING_NUMBER_SENT_COMMENT_C,
    {{ Boolean_Transform('DO_NOT_SEND_AMA_CRUISER_APP_EMAIL_C') }} as DO_NOT_SEND_AMA_CRUISER_APP_EMAIL_C,
    {{ DateTime_Transform('FINALIZE_GROUP_C') }} as FINALIZE_GROUP_C,
    {{ String_Transform('FINALIZE_GROUP_COMMENT_C') }} as FINALIZE_GROUP_COMMENT_C,
    {{ Number_Transform('FINAL_PAYMENT_C') }} as FINAL_PAYMENT_C,
    {{ DateTime_Transform('FINAL_PAYMENT_DATE_C') }} as FINAL_PAYMENT_DATE_C,
    {{ String_Transform('FISCAL') }} as FISCAL,
    {{ Number_Transform('FISCAL_QUARTER') }} as FISCAL_QUARTER,
    {{ Number_Transform('FISCAL_YEAR') }} as FISCAL_YEAR,
    {{ String_Transform('FORECAST_CATEGORY') }} as FORECAST_CATEGORY,
    {{ String_Transform('FORECAST_CATEGORY_NAME') }} as FORECAST_CATEGORY_NAME,
    {{ String_Transform('FULL_CH_C') }} as FULL_CH_C,
    {{ Boolean_Transform('GROUP_CONTRACT_SENT_C') }} as GROUP_CONTRACT_SENT_C,
    {{ String_Transform('GROUP_COORDINATOR_C') }} as GROUP_COORDINATOR_C,
    {{ DateTime_Transform('GROUP_CREATE_DATE_C') }} as GROUP_CREATE_DATE_C,
    {{ String_Transform('GROUP_CUSTOM_REQUESTS_ADDITIONAL_COMMENT_C') }} as GROUP_CUSTOM_REQUESTS_ADDITIONAL_COMMENT_C,
    {{ String_Transform('GROUP_CUSTOM_REQUESTS_C') }} as GROUP_CUSTOM_REQUESTS_C,
    {{ String_Transform('GROUP_ID_C') }} as GROUP_ID_C,
    {{ String_Transform('GROUP_PACKAGE_TYPE_C') }} as GROUP_PACKAGE_TYPE_C,
    {{ String_Transform('GROUP_STATUS_C') }} as GROUP_STATUS_C,
    {{ String_Transform('GROUP_TYPE_CODE_C') }} as GROUP_TYPE_CODE_C,
    {{ String_Transform('GROUP_TYPE_NAME_C') }} as GROUP_TYPE_NAME_C,
    {{ Boolean_Transform('HAS_OPEN_ACTIVITY') }} as HAS_OPEN_ACTIVITY,
    {{ Boolean_Transform('HAS_OPPORTUNITY_LINE_ITEM') }} as HAS_OPPORTUNITY_LINE_ITEM,
    {{ Boolean_Transform('HAS_OVERDUE_TASK') }} as HAS_OVERDUE_TASK,
    {{ String_Transform('ID') }} as ID,
    {{ Boolean_Transform('INITIAL_FOLLOW_UP_WITH_TA_C') }} as INITIAL_FOLLOW_UP_WITH_TA_C,
    {{ Number_Transform('INVOICE_TOTAL_AMOUNT_C') }} as INVOICE_TOTAL_AMOUNT_C,
    {{ DateTime_Transform('ISSUE_GROUP_PROPOSAL_C') }} as ISSUE_GROUP_PROPOSAL_C,
    {{ String_Transform('ISSUE_GROUP_PROPOSAL_COMMENT_C') }} as ISSUE_GROUP_PROPOSAL_COMMENT_C,
    {{ Boolean_Transform('IS_CLOSED') }} as IS_CLOSED,
    {{ Boolean_Transform('IS_DELETED') }} as IS_DELETED,
    {{ Boolean_Transform('IS_WON') }} as IS_WON,
    {{ DateTime_Transform('LAST_ACTIVITY_DATE') }} as LAST_ACTIVITY_DATE,
    {{ String_Transform('LAST_AMOUNT_CHANGED_HISTORY_ID') }} as LAST_AMOUNT_CHANGED_HISTORY_ID,
    {{ String_Transform('LAST_CLOSE_DATE_CHANGED_HISTORY_ID') }} as LAST_CLOSE_DATE_CHANGED_HISTORY_ID,
    {{ String_Transform('LAST_MODIFIED_BY_ID') }} as LAST_MODIFIED_BY_ID,
    {{ DateTime_Transform('LAST_MODIFIED_DATE') }} as LAST_MODIFIED_DATE,
    {{ DateTime_Transform('LAST_REFERENCED_DATE') }} as LAST_REFERENCED_DATE,
    {{ DateTime_Transform('LAST_STAGE_CHANGE_DATE') }} as LAST_STAGE_CHANGE_DATE,
    {{ DateTime_Transform('LAST_VIEWED_DATE') }} as LAST_VIEWED_DATE,
    {{ String_Transform('LEAD_SOURCE') }} as LEAD_SOURCE,
    {{ String_Transform('NAME') }} as NAME,
    {{ String_Transform('NEXT_STEP') }} as NEXT_STEP,
    {{ Number_Transform('NUMBER_OF_BOOKED_PASSENGER_C') }} as NUMBER_OF_BOOKED_PASSENGER_C,
    {{ Number_Transform('OF_BOOKED_PASSANGERS_C') }} as OF_BOOKED_PASSANGERS_C,
    {{ String_Transform('OWNER_ID') }} as OWNER_ID,
    {{ String_Transform('PACKAGE_TYPE_OPPORTUNITY_C') }} as PACKAGE_TYPE_OPPORTUNITY_C,
    {{ String_Transform('PART_C_RMS_C') }} as PART_C_RMS_C,
    {{ Boolean_Transform('POST_SAIL_DATE_FOLLOWUP_EMAIL_SENT_C') }} as POST_SAIL_DATE_FOLLOWUP_EMAIL_SENT_C,
    {{ String_Transform('PRICEBOOK_2_ID') }} as PRICEBOOK_2_ID,
    {{ String_Transform('PROGRAM_ITINERARY_C') }} as PROGRAM_ITINERARY_C,
    {{ Number_Transform('PUSH_COUNT') }} as PUSH_COUNT,
    {{ Boolean_Transform('RECEIVED_DEPOSIT_C') }} as RECEIVED_DEPOSIT_C,
    {{ DateTime_Transform('RECEIVED_SIGNED_CONTRACT_AND_DEPOSIT_C') }} as RECEIVED_SIGNED_CONTRACT_AND_DEPOSIT_C,
    {{ Boolean_Transform('RECEIVED_SIGNED_CONTRACT_C') }} as RECEIVED_SIGNED_CONTRACT_C,
    {{ String_Transform('RECORD_TYPE_ID') }} as RECORD_TYPE_ID,
    {{ DateTime_Transform('REQUEST_2_PRIVATE_EXCURSIONS_C') }} as REQUEST_2_PRIVATE_EXCURSIONS_C,
    {{ String_Transform('REQUEST_2_PRIVATE_EXCURSIONS_COMMENT_C') }} as REQUEST_2_PRIVATE_EXCURSIONS_COMMENT_C,
    {{ Boolean_Transform('REQUEST_GROUP_AIR_C') }} as REQUEST_GROUP_AIR_C,
    {{ Number_Transform('ROOMS_BOOKED_C') }} as ROOMS_BOOKED_C,
    {{ Number_Transform('ROOMS_REQUESTED_C') }} as ROOMS_REQUESTED_C,
    {{ String_Transform('SAILING_C') }} as SAILING_C,
    {{ String_Transform('SECONDARY_AGENCY_C') }} as SECONDARY_AGENCY_C,
    {{ Boolean_Transform('SEND_AMA_CRUISER_APP_EMAIL_C') }} as SEND_AMA_CRUISER_APP_EMAIL_C,
    {{ DateTime_Transform('SEND_VISA_INFORMATION_C') }} as SEND_VISA_INFORMATION_C,
    {{ String_Transform('SEND_VISA_INFORMATION_COMMENT_C') }} as SEND_VISA_INFORMATION_COMMENT_C,
    {{ String_Transform('SPECIAL_INTEREST_GROUP_C') }} as SPECIAL_INTEREST_GROUP_C,
    {{ String_Transform('STAGE_NAME') }} as STAGE_NAME,
    {{ String_Transform('STATUS_C') }} as STATUS_C,
    {{ String_Transform('SYNCED_QUOTE_ID') }} as SYNCED_QUOTE_ID,
    {{ transform_datetime1('SYSTEM_MODSTAMP') }} as SYSTEM_MODSTAMP,
    {{ Number_Transform('TOTAL_OPPORTUNITY_QUANTITY') }} as TOTAL_OPPORTUNITY_QUANTITY,
    {{ String_Transform('TRAVEL_AGENT_C') }} as TRAVEL_AGENT_C,
    {{ String_Transform('TRAVEL_AGENT_NAME_C') }} as TRAVEL_AGENT_NAME_C,
    {{ String_Transform('TYPE') }} as TYPE,
    {{ String_Transform('VESSEL_C') }} as VESSEL_C,
    {{ DateTime_Transform('WELCOME_EMAIL_SENT_C') }} as WELCOME_EMAIL_SENT_C,
    {{ String_Transform('WELCOME_EMAIL_SENT_COMMENT_C') }} as WELCOME_EMAIL_SENT_COMMENT_C,
    {{ String_Transform('WELLNESS_HOST_REQUEST_C') }} as WELLNESS_HOST_REQUEST_C,
    {{ DateTime_Transform('X_120_DAY_RELEASE_DATE_C') }} as X_120_DAY_RELEASE_DATE_C,
    {{ DateTime_Transform('X_180_DAY_RELEASE_DATE_C') }} as X_180_DAY_RELEASE_DATE_C,
    {{ DateTime_Transform('X_2_ND_PAYMENT_DATE_FOR_CHARTER_C') }} as X_2_ND_PAYMENT_DATE_FOR_CHARTER_C,
    {{ DateTime_Transform('X_3_RD_PAYMENT_DATE_FOR_CHARTERS_C') }} as X_3_RD_PAYMENT_DATE_FOR_CHARTERS_C,
    {{ Boolean_Transform('_FIVETRAN_DELETED') }} as _FIVETRAN_DELETED,
    {{ DateTime_Transform('_FIVETRAN_SYNCED') }} as _FIVETRAN_SYNCED
    from {{ source('AMA_DEV_BRNZ_SALESFORCE', 'OPPORTUNITY') }}
),

{# ================================================================
   FILTER FOR INCREMENTAL LOAD
   ================================================================ #}

filtered as (
    select *
    from src
    {% if is_incremental() and not is_full %}
        where coalesce({{ wm_col }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm) }}


    {% endif %}
)

select * from filtered

{% if var('config_lookup_debug', false) %}
    {% do log("Executing SQL for get_config_row: Step 3", info=True) %}
{% endif %}



