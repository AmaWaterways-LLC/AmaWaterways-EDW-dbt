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
                 {% set cfg = get_config_row('8x8', target.database, target.schema, 'REALTIME_QUEUES') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_realtime_queues',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='REALTIME_QUEUES',
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
                 {% set wm_col = get_watermark_column('8x8', target.database, target.schema, 'REALTIME_QUEUES') %}
                 {% set max_wm = compute_max_watermark(this, wm_col) %}
                 {% if max_wm is not none %}
                     {% do update_config_watermark('8x8', target.database, target.schema, 'REALTIME_QUEUES', max_wm) %}
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
    {% set cfg = get_config_row('8x8', target.database, target.schema, 'REALTIME_QUEUES') %}
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
    {{ Number_Transform('ABANDONED_INT_15M') }} as ABANDONED_INT_15M,
    {{ Number_Transform('ABANDONED_INT_30M') }} as ABANDONED_INT_30M,
    {{ Number_Transform('ABANDONED_PERCENTAGE_INT_15M') }} as ABANDONED_PERCENTAGE_INT_15M,
    {{ Number_Transform('ABANDONED_PERCENTAGE_INT_30M') }} as ABANDONED_PERCENTAGE_INT_30M,
    {{ Number_Transform('ABANDONED_PERCENTAGE_TODAY') }} as ABANDONED_PERCENTAGE_TODAY,
    {{ Number_Transform('ABANDONED_TODAY') }} as ABANDONED_TODAY,
    {{ Number_Transform('ACCEPTED_INT_15M') }} as ACCEPTED_INT_15M,
    {{ Number_Transform('ACCEPTED_INT_30M') }} as ACCEPTED_INT_30M,
    {{ Number_Transform('ACCEPTED_IN_SLA_INT_12H') }} as ACCEPTED_IN_SLA_INT_12H,
    {{ Number_Transform('ACCEPTED_IN_SLA_INT_15M') }} as ACCEPTED_IN_SLA_INT_15M,
    {{ Number_Transform('ACCEPTED_IN_SLA_INT_1H') }} as ACCEPTED_IN_SLA_INT_1H,
    {{ Number_Transform('ACCEPTED_IN_SLA_INT_30M') }} as ACCEPTED_IN_SLA_INT_30M,
    {{ Number_Transform('ACCEPTED_IN_SLA_INT_4H') }} as ACCEPTED_IN_SLA_INT_4H,
    {{ Number_Transform('ACCEPTED_IN_SLA_INT_8H') }} as ACCEPTED_IN_SLA_INT_8H,
    {{ Number_Transform('ACCEPTED_IN_SLA_PERCENTAGE_INT_12H') }} as ACCEPTED_IN_SLA_PERCENTAGE_INT_12H,
    {{ Number_Transform('ACCEPTED_IN_SLA_PERCENTAGE_INT_15M') }} as ACCEPTED_IN_SLA_PERCENTAGE_INT_15M,
    {{ Number_Transform('ACCEPTED_IN_SLA_PERCENTAGE_INT_1H') }} as ACCEPTED_IN_SLA_PERCENTAGE_INT_1H,
    {{ Number_Transform('ACCEPTED_IN_SLA_PERCENTAGE_INT_30M') }} as ACCEPTED_IN_SLA_PERCENTAGE_INT_30M,
    {{ Number_Transform('ACCEPTED_IN_SLA_PERCENTAGE_INT_4H') }} as ACCEPTED_IN_SLA_PERCENTAGE_INT_4H,
    {{ Number_Transform('ACCEPTED_IN_SLA_PERCENTAGE_INT_8H') }} as ACCEPTED_IN_SLA_PERCENTAGE_INT_8H,
    {{ Number_Transform('ACCEPTED_IN_SLA_PERCENTAGE_TODAY') }} as ACCEPTED_IN_SLA_PERCENTAGE_TODAY,
    {{ Number_Transform('ACCEPTED_IN_SLA_TODAY') }} as ACCEPTED_IN_SLA_TODAY,
    {{ Number_Transform('ACCEPTED_PERCENTAGE_INT_15M') }} as ACCEPTED_PERCENTAGE_INT_15M,
    {{ Number_Transform('ACCEPTED_PERCENTAGE_INT_30M') }} as ACCEPTED_PERCENTAGE_INT_30M,
    {{ Number_Transform('ACCEPTED_PERCENTAGE_TODAY') }} as ACCEPTED_PERCENTAGE_TODAY,
    {{ Number_Transform('ACCEPTED_TODAY') }} as ACCEPTED_TODAY,
    {{ Number_Transform('AVAILABLE_IDLE_RT') }} as AVAILABLE_IDLE_RT,
    {{ Number_Transform('AVG_DIVERTED_TIME_INT_15M') }} as AVG_DIVERTED_TIME_INT_15M,
    {{ Number_Transform('AVG_DIVERTED_TIME_INT_30M') }} as AVG_DIVERTED_TIME_INT_30M,
    {{ Number_Transform('AVG_DIVERTED_TIME_TODAY') }} as AVG_DIVERTED_TIME_TODAY,
    {{ Number_Transform('AVG_HANDLING_TIME_INT_15M') }} as AVG_HANDLING_TIME_INT_15M,
    {{ Number_Transform('AVG_HANDLING_TIME_INT_30M') }} as AVG_HANDLING_TIME_INT_30M,
    {{ Number_Transform('AVG_HANDLING_TIME_TODAY') }} as AVG_HANDLING_TIME_TODAY,
    {{ Number_Transform('AVG_OFFERING_TIME_INT_15M') }} as AVG_OFFERING_TIME_INT_15M,
    {{ Number_Transform('AVG_OFFERING_TIME_INT_30M') }} as AVG_OFFERING_TIME_INT_30M,
    {{ Number_Transform('AVG_OFFERING_TIME_TODAY') }} as AVG_OFFERING_TIME_TODAY,
    {{ Number_Transform('AVG_PROCESSING_TIME_INT_15M') }} as AVG_PROCESSING_TIME_INT_15M,
    {{ Number_Transform('AVG_PROCESSING_TIME_INT_30M') }} as AVG_PROCESSING_TIME_INT_30M,
    {{ Number_Transform('AVG_PROCESSING_TIME_TODAY') }} as AVG_PROCESSING_TIME_TODAY,
    {{ Number_Transform('AVG_WORK_TIME_INT_15M') }} as AVG_WORK_TIME_INT_15M,
    {{ Number_Transform('AVG_WORK_TIME_INT_30M') }} as AVG_WORK_TIME_INT_30M,
    {{ Number_Transform('AVG_WORK_TIME_TODAY') }} as AVG_WORK_TIME_TODAY,
    {{ Number_Transform('AVG_WRAP_UP_TIME_INT_15M') }} as AVG_WRAP_UP_TIME_INT_15M,
    {{ Number_Transform('AVG_WRAP_UP_TIME_INT_30M') }} as AVG_WRAP_UP_TIME_INT_30M,
    {{ Number_Transform('AVG_WRAP_UP_TIME_TODAY') }} as AVG_WRAP_UP_TIME_TODAY,
    {{ Number_Transform('BUSY_EXTERNAL_RT') }} as BUSY_EXTERNAL_RT,
    {{ Number_Transform('BUSY_OTHER_RT') }} as BUSY_OTHER_RT,
    {{ Number_Transform('BUSY_RT') }} as BUSY_RT,
    {{ Number_Transform('DIVERTED_INT_15M') }} as DIVERTED_INT_15M,
    {{ Number_Transform('DIVERTED_INT_30M') }} as DIVERTED_INT_30M,
    {{ Number_Transform('DIVERTED_PERCENTAGE_INT_15M') }} as DIVERTED_PERCENTAGE_INT_15M,
    {{ Number_Transform('DIVERTED_PERCENTAGE_INT_30M') }} as DIVERTED_PERCENTAGE_INT_30M,
    {{ Number_Transform('DIVERTED_PERCENTAGE_TODAY') }} as DIVERTED_PERCENTAGE_TODAY,
    {{ Number_Transform('DIVERTED_TODAY') }} as DIVERTED_TODAY,
    {{ Number_Transform('ELIGIBLE_RT') }} as ELIGIBLE_RT,
    {{ Number_Transform('ENABLED_RT') }} as ENABLED_RT,
    {{ Number_Transform('ENTERED_INT_15M') }} as ENTERED_INT_15M,
    {{ Number_Transform('ENTERED_INT_30M') }} as ENTERED_INT_30M,
    {{ Number_Transform('ENTERED_TODAY') }} as ENTERED_TODAY,
    {{ Number_Transform('HANDLING_RT') }} as HANDLING_RT,
    {{ String_Transform('ID') }} as ID,
    {{ Number_Transform('INTERACTIONS_AVG_WAIT_TIME_INT_15M') }} as INTERACTIONS_AVG_WAIT_TIME_INT_15M,
    {{ Number_Transform('INTERACTIONS_AVG_WAIT_TIME_INT_30M') }} as INTERACTIONS_AVG_WAIT_TIME_INT_30M,
    {{ Number_Transform('INTERACTIONS_AVG_WAIT_TIME_TODAY') }} as INTERACTIONS_AVG_WAIT_TIME_TODAY,
    {{ Number_Transform('INTERACTIONS_HANDLING_RT') }} as INTERACTIONS_HANDLING_RT,
    {{ Number_Transform('INTERACTIONS_LONGEST_WAIT_IN_QUEUE_INT_15M') }} as INTERACTIONS_LONGEST_WAIT_IN_QUEUE_INT_15M,
    {{ Number_Transform('INTERACTIONS_LONGEST_WAIT_IN_QUEUE_INT_30M') }} as INTERACTIONS_LONGEST_WAIT_IN_QUEUE_INT_30M,
    {{ Number_Transform('INTERACTIONS_LONGEST_WAIT_IN_QUEUE_RT') }} as INTERACTIONS_LONGEST_WAIT_IN_QUEUE_RT,
    {{ Number_Transform('INTERACTIONS_LONGEST_WAIT_IN_QUEUE_TODAY') }} as INTERACTIONS_LONGEST_WAIT_IN_QUEUE_TODAY,
    {{ Number_Transform('INTERACTIONS_WAIT_IN_QUEUE_RT') }} as INTERACTIONS_WAIT_IN_QUEUE_RT,
    {{ Number_Transform('INTERACTIONS_WRAP_UP_RT') }} as INTERACTIONS_WRAP_UP_RT,
    {{ Number_Transform('LONGEST_OFFERING_TIME_IN_QUEUE_INT_15M') }} as LONGEST_OFFERING_TIME_IN_QUEUE_INT_15M,
    {{ Number_Transform('LONGEST_OFFERING_TIME_IN_QUEUE_INT_30M') }} as LONGEST_OFFERING_TIME_IN_QUEUE_INT_30M,
    {{ Number_Transform('LONGEST_OFFERING_TIME_IN_QUEUE_TODAY') }} as LONGEST_OFFERING_TIME_IN_QUEUE_TODAY,
    {{ String_Transform('NAME') }} as NAME,
    {{ Number_Transform('NEW_IN_QUEUE_INT_15M') }} as NEW_IN_QUEUE_INT_15M,
    {{ Number_Transform('NEW_IN_QUEUE_INT_30M') }} as NEW_IN_QUEUE_INT_30M,
    {{ Number_Transform('NEW_IN_QUEUE_TODAY') }} as NEW_IN_QUEUE_TODAY,
    {{ Number_Transform('OFFERING_RT') }} as OFFERING_RT,
    {{ Number_Transform('ON_BREAK_RT') }} as ON_BREAK_RT,
    {{ Number_Transform('SHORT_ABANDONED_INT_15M') }} as SHORT_ABANDONED_INT_15M,
    {{ Number_Transform('SHORT_ABANDONED_INT_30M') }} as SHORT_ABANDONED_INT_30M,
    {{ Number_Transform('SHORT_ABANDONED_PERCENTAGE_INT_15M') }} as SHORT_ABANDONED_PERCENTAGE_INT_15M,
    {{ Number_Transform('SHORT_ABANDONED_PERCENTAGE_INT_30M') }} as SHORT_ABANDONED_PERCENTAGE_INT_30M,
    {{ Number_Transform('SHORT_ABANDONED_PERCENTAGE_TODAY') }} as SHORT_ABANDONED_PERCENTAGE_TODAY,
    {{ Number_Transform('SHORT_ABANDONED_TODAY') }} as SHORT_ABANDONED_TODAY,
    {{ Number_Transform('SLA_PERCENTAGE_INT_12H') }} as SLA_PERCENTAGE_INT_12H,
    {{ Number_Transform('SLA_PERCENTAGE_INT_15M') }} as SLA_PERCENTAGE_INT_15M,
    {{ Number_Transform('SLA_PERCENTAGE_INT_1H') }} as SLA_PERCENTAGE_INT_1H,
    {{ Number_Transform('SLA_PERCENTAGE_INT_30M') }} as SLA_PERCENTAGE_INT_30M,
    {{ Number_Transform('SLA_PERCENTAGE_INT_4H') }} as SLA_PERCENTAGE_INT_4H,
    {{ Number_Transform('SLA_PERCENTAGE_INT_8H') }} as SLA_PERCENTAGE_INT_8H,
    {{ Number_Transform('SLA_PERCENTAGE_TARGET_TODAY') }} as SLA_PERCENTAGE_TARGET_TODAY,
    {{ Number_Transform('SLA_PERCENTAGE_TODAY') }} as SLA_PERCENTAGE_TODAY,
    {{ Number_Transform('SLA_TIME_THRESHOLD_TODAY') }} as SLA_TIME_THRESHOLD_TODAY,
    {{ Number_Transform('TOTAL_ABANDONED_INT_15M') }} as TOTAL_ABANDONED_INT_15M,
    {{ Number_Transform('TOTAL_ABANDONED_INT_30M') }} as TOTAL_ABANDONED_INT_30M,
    {{ Number_Transform('TOTAL_ABANDONED_PERCENTAGE_INT_15M') }} as TOTAL_ABANDONED_PERCENTAGE_INT_15M,
    {{ Number_Transform('TOTAL_ABANDONED_PERCENTAGE_INT_30M') }} as TOTAL_ABANDONED_PERCENTAGE_INT_30M,
    {{ Number_Transform('TOTAL_ABANDONED_PERCENTAGE_TODAY') }} as TOTAL_ABANDONED_PERCENTAGE_TODAY,
    {{ Number_Transform('TOTAL_ABANDONED_TODAY') }} as TOTAL_ABANDONED_TODAY,
    {{ Number_Transform('WORKING_OFFLINE_RT') }} as WORKING_OFFLINE_RT,
    {{ Number_Transform('WRAP_UP_RT') }} as WRAP_UP_RT
    from {{ source('AMA_DEV_BRNZ_8X8', 'REALTIME_QUEUES') }}
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