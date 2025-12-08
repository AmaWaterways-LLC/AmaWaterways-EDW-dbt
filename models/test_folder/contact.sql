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
                 {% set cfg = get_config_row('SALESFORCE', target.database, target.schema, 'CAMPAIGN') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_campaign',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='CAMPAIGN',
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
                 {% set wm_col = get_watermark_column('SALESFORCE', target.database, target.schema, 'CAMPAIGN') %}
                 {% set max_wm = compute_max_watermark(this, wm_col) %}
                 {% if max_wm is not none %}
                     {% do update_config_watermark('SALESFORCE', target.database, target.schema, 'CAMPAIGN', max_wm) %}
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
    {% set cfg = get_config_row('SALESFORCE', target.database, target.schema, 'CAMPAIGN') %}
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
    {{ Number_Transform('ACTUAL_COST') }} as ACTUAL_COST,
    {{ String_Transform('ADDITIONAL_COMMENTS_AWARDS_RECEIVED_DEL_C') }} as ADDITIONAL_COMMENTS_AWARDS_RECEIVED_DEL_C,
    {{ String_Transform('AMA_WATERWAYS_REPRESENTATIVE_S_DEL_C') }} as AMA_WATERWAYS_REPRESENTATIVE_S_DEL_C,
    {{ Number_Transform('AMOUNT_ALL_OPPORTUNITIES') }} as AMOUNT_ALL_OPPORTUNITIES,
    {{ Number_Transform('AMOUNT_WON_OPPORTUNITIES') }} as AMOUNT_WON_OPPORTUNITIES,
    {{ String_Transform('ATTENDANCE_DEL_C') }} as ATTENDANCE_DEL_C,
    {{ Number_Transform('BUDGETED_COST') }} as BUDGETED_COST,
    {{ String_Transform('CAMPAIGN_C') }} as CAMPAIGN_C,
    {{ String_Transform('CAMPAIGN_IMAGE_ID') }} as CAMPAIGN_IMAGE_ID,
    {{ String_Transform('CAMPAIGN_MEMBER_RECORD_TYPE_ID') }} as CAMPAIGN_MEMBER_RECORD_TYPE_ID,
    {{ String_Transform('CAMPAIGN_TYPE_C') }} as CAMPAIGN_TYPE_C,
    {{ String_Transform('CITY_DEL_C') }} as CITY_DEL_C,
    {{ DateTime_Transform('COLLATERAL_DUE_DATE_C') }} as COLLATERAL_DUE_DATE_C,
    {{ String_Transform('COLLATERAL_NEEDS_DEL_C') }} as COLLATERAL_NEEDS_DEL_C,
    {{ Number_Transform('COLLATORAL_QUANTITIES_C') }} as COLLATORAL_QUANTITIES_C,
    {{ String_Transform('CONSORTIA_C') }} as CONSORTIA_C,
    {{ Number_Transform('COST_OF_REGISTRATION_C') }} as COST_OF_REGISTRATION_C,
    {{ String_Transform('CREATED_BY_ID') }} as CREATED_BY_ID,
    {{ DateTime_Transform('CREATED_DATE') }} as CREATED_DATE,
    {{ String_Transform('DESCRIPTION') }} as DESCRIPTION,
    {{ DateTime_Transform('END_DATE') }} as END_DATE,
    {{ Number_Transform('ESTIMATED_NUMBER_OF_PARTICIPANTS_C') }} as ESTIMATED_NUMBER_OF_PARTICIPANTS_C,
    {{ Number_Transform('EXPECTED_RESPONSE') }} as EXPECTED_RESPONSE,
    {{ Number_Transform('EXPECTED_REVENUE') }} as EXPECTED_REVENUE,
    {{ String_Transform('ID') }} as ID,
    {{ Boolean_Transform('IS_ACTIVE') }} as IS_ACTIVE,
    {{ Boolean_Transform('IS_DELETED') }} as IS_DELETED,
    {{ DateTime_Transform('LAST_ACTIVITY_DATE') }} as LAST_ACTIVITY_DATE,
    {{ String_Transform('LAST_MODIFIED_BY_ID') }} as LAST_MODIFIED_BY_ID,
    {{ DateTime_Transform('LAST_MODIFIED_DATE') }} as LAST_MODIFIED_DATE,
    {{ DateTime_Transform('LAST_REFERENCED_DATE') }} as LAST_REFERENCED_DATE,
    {{ DateTime_Transform('LAST_VIEWED_DATE') }} as LAST_VIEWED_DATE,
    {{ String_Transform('NAME') }} as NAME,
    {{ Boolean_Transform('NATIONAL_ACCOUNTS_CAMPAIGN_C') }} as NATIONAL_ACCOUNTS_CAMPAIGN_C,
    {{ Number_Transform('NUMBER_OF_CONTACTS') }} as NUMBER_OF_CONTACTS,
    {{ Number_Transform('NUMBER_OF_CONVERTED_LEADS') }} as NUMBER_OF_CONVERTED_LEADS,
    {{ Number_Transform('NUMBER_OF_LEADS') }} as NUMBER_OF_LEADS,
    {{ Number_Transform('NUMBER_OF_OPPORTUNITIES') }} as NUMBER_OF_OPPORTUNITIES,
    {{ Number_Transform('NUMBER_OF_RESPONSES') }} as NUMBER_OF_RESPONSES,
    {{ Number_Transform('NUMBER_OF_WON_OPPORTUNITIES') }} as NUMBER_OF_WON_OPPORTUNITIES,
    {{ Number_Transform('NUMBER_SENT') }} as NUMBER_SENT,
    {{ String_Transform('OWNER_ID') }} as OWNER_ID,
    {{ String_Transform('PARENT_ID') }} as PARENT_ID,
    {{ String_Transform('PARTICIPANT_TYPE_C') }} as PARTICIPANT_TYPE_C,
    {{ String_Transform('PRESENTATION_DEL_C') }} as PRESENTATION_DEL_C,
    {{ DateTime_Transform('PRESENTATION_DUE_DATE_C') }} as PRESENTATION_DUE_DATE_C,
    {{ String_Transform('RECORD_TYPE_ID') }} as RECORD_TYPE_ID,
    {{ String_Transform('REGISTERED_C') }} as REGISTERED_C,
    {{ DateTime_Transform('START_DATE') }} as START_DATE,
    {{ String_Transform('STATE_C') }} as STATE_C,
    {{ String_Transform('STATUS') }} as STATUS,
    {{ Boolean_Transform('STRATEGIC_NETWORK_CAMPAIGN_C') }} as STRATEGIC_NETWORK_CAMPAIGN_C,
    {{ DateTime_Transform('SYSTEM_MODSTAMP') }} as SYSTEM_MODSTAMP,
    {{ String_Transform('THEME_OF_CONFERENCE_DEL_C') }} as THEME_OF_CONFERENCE_DEL_C,
    {{ String_Transform('TYPE') }} as TYPE,
    {{ Boolean_Transform('_FIVETRAN_DELETED') }} as _FIVETRAN_DELETED,
    {{ DateTime_Transform('_FIVETRAN_SYNCED') }} as _FIVETRAN_SYNCED
    from {{ source('AMA_DEV_BRNZ_SALESFORCE', 'CAMPAIGN') }}
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