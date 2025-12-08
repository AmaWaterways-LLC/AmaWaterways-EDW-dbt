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
                 {% set cfg = get_config_row('SALESFORCE', target.database, target.schema, 'BOOKING_C') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_booking_c',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='BOOKING_C',
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
                 {% set wm_col = get_watermark_column('SALESFORCE', target.database, target.schema, 'BOOKING_C') %}
                 {% set max_wm = compute_max_watermark(this, wm_col) %}
                 {% if max_wm is not none %}
                     {% do update_config_watermark('SALESFORCE', target.database, target.schema, 'BOOKING_C', max_wm) %}
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
    {% set cfg = get_config_row('SALESFORCE', target.database, target.schema, 'BOOKING_C') %}
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
    {{ String_Transform('AGENCY_C') }} as AGENCY_C,
    {{ String_Transform('BERTH_CATEGORY_C') }} as BERTH_CATEGORY_C,
    {{ String_Transform('BOOKING_CANCELLATION_REASON_C') }} as BOOKING_CANCELLATION_REASON_C,
    {{ DateTime_Transform('BOOKING_DATE_C') }} as BOOKING_DATE_C,
    {{ String_Transform('BOOKING_NUMBER_C') }} as BOOKING_NUMBER_C,
    {{ String_Transform('BOOKING_PACKAGE_TYPE_C') }} as BOOKING_PACKAGE_TYPE_C,
    {{ String_Transform('BOOKING_SOURCE_C') }} as BOOKING_SOURCE_C,
    {{ String_Transform('BOOKING_STATUS_C') }} as BOOKING_STATUS_C,
    {{ Number_Transform('CABIN_C') }} as CABIN_C,
    {{ String_Transform('CAMPAIGN_RELATED_C') }} as CAMPAIGN_RELATED_C,
    {{ String_Transform('CAMPAIGN_SOURCE_C') }} as CAMPAIGN_SOURCE_C,
    {{ String_Transform('CATEGORY_C') }} as CATEGORY_C,
    {{ String_Transform('COMMENTS_C') }} as COMMENTS_C,
    {{ Number_Transform('COMMISSION_C') }} as COMMISSION_C,
    {{ String_Transform('CONTACT_OWNER_EMAIL_ADDRESS_C') }} as CONTACT_OWNER_EMAIL_ADDRESS_C,
    {{ String_Transform('CREATED_BY_ID') }} as CREATED_BY_ID,
    {{ DateTime_Transform('CREATED_DATE') }} as CREATED_DATE,
    {{ String_Transform('CRUISE_NIGHTS_C') }} as CRUISE_NIGHTS_C,
    {{ DateTime_Transform('DEPOSIT_DUE_DATE_C') }} as DEPOSIT_DUE_DATE_C,
    {{ String_Transform('FCC_115_AMOUNT_C') }} as FCC_115_AMOUNT_C,
    {{ String_Transform('GROUP_ID_C') }} as GROUP_ID_C,
    {{ String_Transform('GUEST_SEQUENCE_NO_1_C') }} as GUEST_SEQUENCE_NO_1_C,
    {{ String_Transform('GUEST_SEQUENCE_NO_2_C') }} as GUEST_SEQUENCE_NO_2_C,
    {{ String_Transform('GUEST_SEQUENCE_NO_3_C') }} as GUEST_SEQUENCE_NO_3_C,
    {{ String_Transform('GUEST_SEQUENCE_NO_4_C') }} as GUEST_SEQUENCE_NO_4_C,
    {{ String_Transform('ID') }} as ID,
    {{ Boolean_Transform('IS_DELETED') }} as IS_DELETED,
    {{ String_Transform('ITINERARY_NAME_C') }} as ITINERARY_NAME_C,
    {{ DateTime_Transform('LAST_ACTIVITY_DATE') }} as LAST_ACTIVITY_DATE,
    {{ String_Transform('LAST_MODIFIED_BY_ID') }} as LAST_MODIFIED_BY_ID,
    {{ DateTime_Transform('LAST_MODIFIED_DATE') }} as LAST_MODIFIED_DATE,
    {{ DateTime_Transform('LAST_REFERENCED_DATE') }} as LAST_REFERENCED_DATE,
    {{ DateTime_Transform('LAST_VIEWED_DATE') }} as LAST_VIEWED_DATE,
    {{ String_Transform('MARKETING_PROMOTIONS_C') }} as MARKETING_PROMOTIONS_C,
    {{ String_Transform('NAME') }} as NAME,
    {{ Number_Transform('NET_CRUISE_ONLY_C') }} as NET_CRUISE_ONLY_C,
    {{ Number_Transform('NUMBER_OF_PASSENGERS_FIT_C') }} as NUMBER_OF_PASSENGERS_FIT_C,
    {{ Number_Transform('NUMBER_OF_PASSENGERS_GROUP_C') }} as NUMBER_OF_PASSENGERS_GROUP_C,
    {{ Number_Transform('OPEN_BALANCE_C') }} as OPEN_BALANCE_C,
    {{ String_Transform('OPPORTUNITY_C') }} as OPPORTUNITY_C,
    {{ String_Transform('PACKAGE_CODE_C') }} as PACKAGE_CODE_C,
    {{ Number_Transform('PACKAGE_ID_C') }} as PACKAGE_ID_C,
    {{ Number_Transform('PAYMENTS_C') }} as PAYMENTS_C,
    {{ DateTime_Transform('PAYMENT_DUE_DATE_C') }} as PAYMENT_DUE_DATE_C,
    {{ Number_Transform('PORT_CHARGE_C') }} as PORT_CHARGE_C,
    {{ String_Transform('POST_DAYS_C') }} as POST_DAYS_C,
    {{ String_Transform('PRC_AGENT_C') }} as PRC_AGENT_C,
    {{ String_Transform('PRE_DAYS_C') }} as PRE_DAYS_C,
    {{ String_Transform('PRIMARY_PASSENGER_C') }} as PRIMARY_PASSENGER_C,
    {{ String_Transform('PROMOS_C') }} as PROMOS_C,
    {{ String_Transform('RESERVATION_MANAGER_C') }} as RESERVATION_MANAGER_C,
    {{ String_Transform('RES_FIT_AGENT_C') }} as RES_FIT_AGENT_C,
    {{ String_Transform('RIVER_C') }} as RIVER_C,
    {{ String_Transform('SAILING_CODE_C') }} as SAILING_CODE_C,
    {{ String_Transform('SAILING_NAME_C') }} as SAILING_NAME_C,
    {{ Number_Transform('SAIL_ID_BOOKING_C') }} as SAIL_ID_BOOKING_C,
    {{ String_Transform('SEASON_PROMO_C') }} as SEASON_PROMO_C,
    {{ String_Transform('SECONDARY_AGENCY_C') }} as SECONDARY_AGENCY_C,
    {{ String_Transform('SHIP_BOOKING_C') }} as SHIP_BOOKING_C,
    {{ Boolean_Transform('STATUS_CHANGED_FROM_OF_TO_BK_C') }} as STATUS_CHANGED_FROM_OF_TO_BK_C,
    {{ DateTime_Transform('SYSTEM_MODSTAMP') }} as SYSTEM_MODSTAMP,
    {{ Number_Transform('TOTAL_GROSS_REVENUE_COMBINED_C') }} as TOTAL_GROSS_REVENUE_COMBINED_C,
    {{ Number_Transform('TOTAL_NET_REVENUE_COMBINED_C') }} as TOTAL_NET_REVENUE_COMBINED_C,
    {{ String_Transform('TRAVEL_AGENT_C') }} as TRAVEL_AGENT_C,
    {{ String_Transform('UPDATE_STATUS_C') }} as UPDATE_STATUS_C,
    {{ DateTime_Transform('VACATION_END_DATE_C') }} as VACATION_END_DATE_C,
    {{ DateTime_Transform('VACATION_START_DATE_C') }} as VACATION_START_DATE_C,
    {{ Boolean_Transform('_FIVETRAN_DELETED') }} as _FIVETRAN_DELETED,
    {{ DateTime_Transform('_FIVETRAN_SYNCED') }} as _FIVETRAN_SYNCED
    from {{ source('AMA_DEV_BRNZ_SALESFORCE', 'BOOKING_C') }}
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