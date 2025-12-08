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
                 {% set cfg = get_config_row('SALESFORCE', target.database, target.schema, 'ACCOUNT') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_account',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='ACCOUNT',
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
                 {% set wm_col = get_watermark_column('SALESFORCE', target.database, target.schema, 'ACCOUNT') %}
                 {% set max_wm = compute_max_watermark(this, wm_col) %}
                 {% if max_wm is not none %}
                     {% do update_config_watermark('SALESFORCE', target.database, target.schema, 'ACCOUNT', max_wm) %}
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
    {% set cfg = get_config_row('SALESFORCE', target.database, target.schema, 'ACCOUNT') %}
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
    {{ String_Transform('ACCOUNT_NOTES_C') }} as ACCOUNT_NOTES_C,
    {{ String_Transform('ACCOUNT_NUMBER') }} as ACCOUNT_NUMBER,
    {{ String_Transform('BILLING_CITY') }} as BILLING_CITY,
    {{ String_Transform('BILLING_COUNTRY') }} as BILLING_COUNTRY,
    {{ String_Transform('BILLING_GEOCODE_ACCURACY') }} as BILLING_GEOCODE_ACCURACY,
    {{ Number_Transform('BILLING_LATITUDE') }} as BILLING_LATITUDE,
    {{ Number_Transform('BILLING_LONGITUDE') }} as BILLING_LONGITUDE,
    {{ String_Transform('BILLING_POSTAL_CODE') }} as BILLING_POSTAL_CODE,
    {{ String_Transform('BILLING_STATE') }} as BILLING_STATE,
    {{ String_Transform('BILLING_STREET') }} as BILLING_STREET,
    {{ String_Transform('CAMPAIGN_CODE_C') }} as CAMPAIGN_CODE_C,
    {{ String_Transform('CAMPAIGN_RELATED_C') }} as CAMPAIGN_RELATED_C,
    {{ String_Transform('CHANNEL_PROGRAM_LEVEL_NAME') }} as CHANNEL_PROGRAM_LEVEL_NAME,
    {{ String_Transform('CHANNEL_PROGRAM_NAME') }} as CHANNEL_PROGRAM_NAME,
    {{ String_Transform('CLIA_C') }} as CLIA_C,
    {{ String_Transform('CODE_C') }} as CODE_C,
    {{ String_Transform('COMPANY_LOGO_C') }} as COMPANY_LOGO_C,
    {{ String_Transform('COMPETITOR_WEAKNESSES_C') }} as COMPETITOR_WEAKNESSES_C,
    {{ String_Transform('CREATED_BY_ID') }} as CREATED_BY_ID,
    {{ DateTime_Transform('CREATED_DATE') }} as CREATED_DATE,
    {{ String_Transform('CURRENCY_C') }} as CURRENCY_C,
    {{ String_Transform('DESCRIPTION') }} as DESCRIPTION,
    {{ String_Transform('DISTRICT_ID_C') }} as DISTRICT_ID_C,
    {{ Boolean_Transform('DO_NOT_MAIL_C') }} as DO_NOT_MAIL_C,
    {{ String_Transform('EMAIL_C') }} as EMAIL_C,
    {{ String_Transform('FAX') }} as FAX,
    {{ String_Transform('GENERAL_INFORMATION_C') }} as GENERAL_INFORMATION_C,
    {{ Number_Transform('GROSS_REVENUE_BOOKED_CURRENT_YEAR_C') }} as GROSS_REVENUE_BOOKED_CURRENT_YEAR_C,
    {{ Number_Transform('GROSS_REVENUE_BOOKED_PRIOR_YEAR_C') }} as GROSS_REVENUE_BOOKED_PRIOR_YEAR_C,
    {{ String_Transform('HOST_ACCOUNT_C') }} as HOST_ACCOUNT_C,
    {{ String_Transform('HOUSEHOLD_ID_C') }} as HOUSEHOLD_ID_C,
    {{ Boolean_Transform('HOUSE_ACCOUNT_C') }} as HOUSE_ACCOUNT_C,
    {{ String_Transform('IATA_C') }} as IATA_C,
    {{ String_Transform('ID') }} as ID,
    {{ String_Transform('INDUSTRY') }} as INDUSTRY,
    {{ Boolean_Transform('IS_DELETED') }} as IS_DELETED,
    {{ Boolean_Transform('IS_PARTNER') }} as IS_PARTNER,
    {{ Boolean_Transform('IS_PRIORITY_RECORD') }} as IS_PRIORITY_RECORD,
    {{ String_Transform('JIGSAW_COMPANY_ID') }} as JIGSAW_COMPANY_ID,
    {{ Boolean_Transform('KEY_ACCOUNTS_C') }} as KEY_ACCOUNTS_C,
    {{ DateTime_Transform('LAST_ACTIVITY_DATE') }} as LAST_ACTIVITY_DATE,
    {{ String_Transform('LAST_MODIFIED_BY_ID') }} as LAST_MODIFIED_BY_ID,
    {{ DateTime_Transform('LAST_MODIFIED_DATE') }} as LAST_MODIFIED_DATE,
    {{ DateTime_Transform('LAST_REFERENCED_DATE') }} as LAST_REFERENCED_DATE,
    {{ DateTime_Transform('LAST_VIEWED_DATE') }} as LAST_VIEWED_DATE,
    {{ Number_Transform('MARKETING_MEDIA_ACCOUNT_C') }} as MARKETING_MEDIA_ACCOUNT_C,
    {{ String_Transform('MASTER_RECORD_ID') }} as MASTER_RECORD_ID,
    {{ String_Transform('NAME') }} as NAME,
    {{ String_Transform('NATIONAL_ACCOUNTS_MANAGER_NOTES_C') }} as NATIONAL_ACCOUNTS_MANAGER_NOTES_C,
    {{ Number_Transform('NET_REVENUE_BOOKED_CURRENT_YEAR_C') }} as NET_REVENUE_BOOKED_CURRENT_YEAR_C,
    {{ Number_Transform('NET_REVENUE_BOOKED_PRIOR_YEAR_C') }} as NET_REVENUE_BOOKED_PRIOR_YEAR_C,
    {{ Boolean_Transform('NON_PRODUCER_C') }} as NON_PRODUCER_C,
    {{ String_Transform('NOTIFY_UPDATE_FOR_INSIDE_SALES_C') }} as NOTIFY_UPDATE_FOR_INSIDE_SALES_C,
    {{ Number_Transform('NUMBER_OF_EMPLOYEES') }} as NUMBER_OF_EMPLOYEES,
    {{ String_Transform('OFFICE_CODE_C') }} as OFFICE_CODE_C,
    {{ String_Transform('OTHER_PHONE_C') }} as OTHER_PHONE_C,
    {{ String_Transform('OUR_ADVANTAGES_C') }} as OUR_ADVANTAGES_C,
    {{ String_Transform('OWNERSHIP') }} as OWNERSHIP,
    {{ String_Transform('OWNER_ID') }} as OWNER_ID,
    {{ String_Transform('PARENT_ID') }} as PARENT_ID,
    {{ String_Transform('PHONE') }} as PHONE,
    {{ String_Transform('PHONE_EXTENSION_C') }} as PHONE_EXTENSION_C,
    {{ String_Transform('PHOTO_URL') }} as PHOTO_URL,
    {{ String_Transform('RECORD_TYPE_ID') }} as RECORD_TYPE_ID,
    {{ String_Transform('RESCO_AGENCY_ID_C') }} as RESCO_AGENCY_ID_C,
    {{ Number_Transform('SAILED_OR_SET_TO_SAIL_LAST_YEAR_C') }} as SAILED_OR_SET_TO_SAIL_LAST_YEAR_C,
    {{ Number_Transform('SAILED_OR_SET_TO_SAIL_NEXT_YEAR_C') }} as SAILED_OR_SET_TO_SAIL_NEXT_YEAR_C,
    {{ Number_Transform('SAILED_OR_SET_TO_SAIL_THIS_YEAR_C') }} as SAILED_OR_SET_TO_SAIL_THIS_YEAR_C,
    {{ String_Transform('SEAWARE_AGENCY_ID_C') }} as SEAWARE_AGENCY_ID_C,
    {{ String_Transform('SECONDARY_ACCOUNT_NAME_C') }} as SECONDARY_ACCOUNT_NAME_C,
    {{ String_Transform('SELLING_TIPS_C') }} as SELLING_TIPS_C,
    {{ Boolean_Transform('SHARED_ACCOUNT_C') }} as SHARED_ACCOUNT_C,
    {{ String_Transform('SHIPPING_CITY') }} as SHIPPING_CITY,
    {{ String_Transform('SHIPPING_COUNTRY') }} as SHIPPING_COUNTRY,
    {{ String_Transform('SHIPPING_GEOCODE_ACCURACY') }} as SHIPPING_GEOCODE_ACCURACY,
    {{ Number_Transform('SHIPPING_LATITUDE') }} as SHIPPING_LATITUDE,
    {{ Number_Transform('SHIPPING_LONGITUDE') }} as SHIPPING_LONGITUDE,
    {{ String_Transform('SHIPPING_POSTAL_CODE') }} as SHIPPING_POSTAL_CODE,
    {{ String_Transform('SHIPPING_STATE') }} as SHIPPING_STATE,
    {{ String_Transform('SHIPPING_STREET') }} as SHIPPING_STREET,
    {{ String_Transform('STATUS_C') }} as STATUS_C,
    {{ DateTime_Transform('STATUS_UPDATED_C') }} as STATUS_UPDATED_C,
    {{ Boolean_Transform('STRATEGIC_ACCOUNT_C') }} as STRATEGIC_ACCOUNT_C,
    {{ DateTime_Transform('SYSTEM_MODSTAMP') }} as SYSTEM_MODSTAMP,
    {{ Number_Transform('TOTAL_GROSS_REVENUE_BOOKED_C') }} as TOTAL_GROSS_REVENUE_BOOKED_C,
    {{ Number_Transform('TOTAL_MARKETING_CO_OP_MONEY_THIS_YEA_DEL_C') }} as TOTAL_MARKETING_CO_OP_MONEY_THIS_YEA_DEL_C,
    {{ String_Transform('TYPE') }} as TYPE,
    {{ String_Transform('WEBSITE') }} as WEBSITE,
    {{ Boolean_Transform('_FIVETRAN_DELETED') }} as _FIVETRAN_DELETED,
    {{ DateTime_Transform('_FIVETRAN_SYNCED') }} as _FIVETRAN_SYNCED
    from {{ source('AMA_DEV_BRNZ_SALESFORCE', 'ACCOUNT') }}
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