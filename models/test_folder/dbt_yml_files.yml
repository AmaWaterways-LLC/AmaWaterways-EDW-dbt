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
        unique_key=['INTERACTION_DURATION'],
        on_schema_change="sync_all_columns",
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('8x8', target.database, target.schema, 'INTERACTION_DETAILS') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_interaction_details',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='INTERACTION_DETAILS',
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
                 {% set wm_col = get_watermark_column('8x8', target.database, target.schema, 'INTERACTION_DETAILS') %}
                 {% set max_wm = compute_max_watermark(this, wm_col) %}
                 {% if max_wm is not none %}
                     {% do update_config_watermark('8x8', target.database, target.schema, 'INTERACTION_DETAILS', max_wm) %}
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
    {% set cfg = get_config_row('8x8', target.database, target.schema, 'INTERACTION_DETAILS') %}
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
    {{ String_Transform('AGENT_NOTES') }} as AGENT_NOTES,
    {{ Number_Transform('BLIND_TRANSFER_TO_AGENT') }} as BLIND_TRANSFER_TO_AGENT,
    {{ Number_Transform('BLIND_TRANSFER_TO_QUEUE') }} as BLIND_TRANSFER_TO_QUEUE,
    {{ String_Transform('CAMPAIGN_ID') }} as CAMPAIGN_ID,
    {{ String_Transform('CAMPAIGN_NAME') }} as CAMPAIGN_NAME,
    {{ String_Transform('CASE_FOLLOW_UP') }} as CASE_FOLLOW_UP,
    {{ String_Transform('CASE_NUMBER') }} as CASE_NUMBER,
    {{ String_Transform('CHANNEL_ID') }} as CHANNEL_ID,
    {{ String_Transform('CHANNEL_NAME') }} as CHANNEL_NAME,
    {{ String_Transform('CHAT_TYPE') }} as CHAT_TYPE,
    {{ Number_Transform('CONFERENCES_ESTABLISHED') }} as CONFERENCES_ESTABLISHED,
    {{ Number_Transform('CONSULTATIONS_ESTABLISHED') }} as CONSULTATIONS_ESTABLISHED,
    {{ DateTime_Transform('CREATION_TIME') }} as CREATION_TIME,
    {{ Number_Transform('CUSTOMER_JOURNEY_DURATION') }} as CUSTOMER_JOURNEY_DURATION,
    {{ String_Transform('CUSTOMER_NAME') }} as CUSTOMER_NAME,
    {{ String_Transform('DESTINATION') }} as DESTINATION,
    {{ String_Transform('DIRECTION') }} as DIRECTION,
    {{ String_Transform('DISPOSITION_ACTION') }} as DISPOSITION_ACTION,
    {{ String_Transform('EXTERNAL_TRANSACTION_DATA') }} as EXTERNAL_TRANSACTION_DATA,
    {{ String_Transform('FACEBOOK_ID') }} as FACEBOOK_ID,
    {{ DateTime_Transform('FINISHED_TIME') }} as FINISHED_TIME,
    {{ Number_Transform('INTERACTION_DURATION') }} as INTERACTION_DURATION,
    {{ String_Transform('INTERACTION_ID') }} as INTERACTION_ID,
    {{ String_Transform('INTERACTION_LABELS') }} as INTERACTION_LABELS,
    {{ String_Transform('INTERACTION_TYPE') }} as INTERACTION_TYPE,
    {{ Number_Transform('IVR_TREATMENT_DURATION') }} as IVR_TREATMENT_DURATION,
    {{ String_Transform('MEDIA_TYPE') }} as MEDIA_TYPE,
    {{ String_Transform('ORIGINAL_INTERACTION_ID') }} as ORIGINAL_INTERACTION_ID,
    {{ String_Transform('ORIGINAL_TRANSACTION_ID') }} as ORIGINAL_TRANSACTION_ID,
    {{ String_Transform('ORIGINATION') }} as ORIGINATION,
    {{ String_Transform('OUTBOUND_PHONE_CODE') }} as OUTBOUND_PHONE_CODE,
    {{ String_Transform('OUTBOUND_PHONE_CODE_ID') }} as OUTBOUND_PHONE_CODE_ID,
    {{ String_Transform('OUTBOUND_PHONE_CODE_LIST') }} as OUTBOUND_PHONE_CODE_LIST,
    {{ String_Transform('OUTBOUND_PHONE_CODE_LIST_ID') }} as OUTBOUND_PHONE_CODE_LIST_ID,
    {{ String_Transform('OUTBOUND_PHONE_CODE_TEXT') }} as OUTBOUND_PHONE_CODE_TEXT,
    {{ String_Transform('OUTBOUND_PHONE_SHORT_CODE') }} as OUTBOUND_PHONE_SHORT_CODE,
    {{ String_Transform('OUTCOME') }} as OUTCOME,
    {{ String_Transform('PARTICIPANT_ASSIGN_NUMBER') }} as PARTICIPANT_ASSIGN_NUMBER,
    {{ Number_Transform('PARTICIPANT_BUSY_DURATION') }} as PARTICIPANT_BUSY_DURATION,
    {{ String_Transform('PARTICIPANT_GROUP_ID') }} as PARTICIPANT_GROUP_ID,
    {{ String_Transform('PARTICIPANT_GROUP_NAME') }} as PARTICIPANT_GROUP_NAME,
    {{ Number_Transform('PARTICIPANT_HANDLING_DURATION') }} as PARTICIPANT_HANDLING_DURATION,
    {{ DateTime_Transform('PARTICIPANT_HANDLING_END_TIME') }} as PARTICIPANT_HANDLING_END_TIME,
    {{ Number_Transform('PARTICIPANT_HOLD') }} as PARTICIPANT_HOLD,
    {{ Number_Transform('PARTICIPANT_HOLD_DURATION') }} as PARTICIPANT_HOLD_DURATION,
    {{ String_Transform('PARTICIPANT_ID') }} as PARTICIPANT_ID,
    {{ Number_Transform('PARTICIPANT_LONGEST_HOLD_DURATION') }} as PARTICIPANT_LONGEST_HOLD_DURATION,
    {{ Number_Transform('PARTICIPANT_LONGEST_MUTE_DURATION') }} as PARTICIPANT_LONGEST_MUTE_DURATION,
    {{ Number_Transform('PARTICIPANT_MUTE') }} as PARTICIPANT_MUTE,
    {{ Number_Transform('PARTICIPANT_MUTE_DURATION') }} as PARTICIPANT_MUTE_DURATION,
    {{ String_Transform('PARTICIPANT_NAME') }} as PARTICIPANT_NAME,
    {{ String_Transform('PARTICIPANT_OFFER_ACTION') }} as PARTICIPANT_OFFER_ACTION,
    {{ DateTime_Transform('PARTICIPANT_OFFER_ACTION_TIME') }} as PARTICIPANT_OFFER_ACTION_TIME,
    {{ Number_Transform('PARTICIPANT_OFFER_DURATION') }} as PARTICIPANT_OFFER_DURATION,
    {{ DateTime_Transform('PARTICIPANT_OFFER_TIME') }} as PARTICIPANT_OFFER_TIME,
    {{ Number_Transform('PARTICIPANT_PROCESSING_DURATION') }} as PARTICIPANT_PROCESSING_DURATION,
    {{ String_Transform('PARTICIPANT_TYPE') }} as PARTICIPANT_TYPE,
    {{ Number_Transform('PARTICIPANT_WRAP_UP_DURATION') }} as PARTICIPANT_WRAP_UP_DURATION,
    {{ DateTime_Transform('PARTICIPANT_WRAP_UP_END_TIME') }} as PARTICIPANT_WRAP_UP_END_TIME,
    {{ String_Transform('QUEUE_ID') }} as QUEUE_ID,
    {{ String_Transform('QUEUE_NAME') }} as QUEUE_NAME,
    {{ DateTime_Transform('QUEUE_TIME') }} as QUEUE_TIME,
    {{ Number_Transform('QUEUE_WAIT_DURATION') }} as QUEUE_WAIT_DURATION,
    {{ String_Transform('RECORDING_FILE_NAMES') }} as RECORDING_FILE_NAMES,
    {{ String_Transform('RECORD_ID') }} as RECORD_ID,
    {{ String_Transform('SCHEDULE_HOURS') }} as SCHEDULE_HOURS,
    {{ String_Transform('STATE') }} as STATE,
    {{ String_Transform('TERMINATED_BY') }} as TERMINATED_BY,
    {{ DateTime_Transform('TIME') }} as TIME,
    {{ String_Transform('TIME_TO_ABANDON') }} as TIME_TO_ABANDON,
    {{ Number_Transform('TOTAL') }} as TOTAL,
    {{ String_Transform('TRANSACTION_ID') }} as TRANSACTION_ID,
    {{ String_Transform('TRANSFERS') }} as TRANSFERS,
    {{ String_Transform('TWITTER_ID') }} as TWITTER_ID,
    {{ Number_Transform('WARM_TRANSFERS_COMPLETED') }} as WARM_TRANSFERS_COMPLETED,
    {{ String_Transform('WRAP_UP_CODE') }} as WRAP_UP_CODE,
    {{ String_Transform('WRAP_UP_CODE_ID') }} as WRAP_UP_CODE_ID,
    {{ String_Transform('WRAP_UP_CODE_LIST') }} as WRAP_UP_CODE_LIST,
    {{ String_Transform('WRAP_UP_CODE_LIST_ID') }} as WRAP_UP_CODE_LIST_ID,
    {{ String_Transform('WRAP_UP_CODE_TEXT') }} as WRAP_UP_CODE_TEXT,
    {{ String_Transform('WRAP_UP_SHORT_CODE') }} as WRAP_UP_SHORT_CODE
    from {{ source('AMA_DEV_BRNZ_8X8', 'INTERACTION_DETAILS') }}
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