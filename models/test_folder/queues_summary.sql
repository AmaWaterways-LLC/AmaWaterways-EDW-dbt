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
        unique_key=['QUEUE_ID'],
        on_schema_change="sync_all_columns",
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('8x8', target.database, target.schema, 'QUEUES_SUMMARY') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_queues_summary',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='QUEUES_SUMMARY',
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
                 {% set wm_col = get_watermark_column('8x8', target.database, target.schema, 'QUEUES_SUMMARY') %}
                 {% set max_wm = compute_max_watermark(this, wm_col) %}
                 {% if max_wm is not none %}
                     {% do update_config_watermark('8x8', target.database, target.schema, 'QUEUES_SUMMARY', max_wm) %}
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
    {% set cfg = get_config_row('8x8', target.database, target.schema, 'QUEUES_SUMMARY') %}
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
    {{ Number_Transform('ACCEPTED') }} as ACCEPTED,
    {{ Number_Transform('ACCEPTED_IN_SLA') }} as ACCEPTED_IN_SLA,
    {{ Number_Transform('ACCEPTED_IN_SLA_PERCENTAGE') }} as ACCEPTED_IN_SLA_PERCENTAGE,
    {{ Number_Transform('ACCEPTED_PERCENTAGE') }} as ACCEPTED_PERCENTAGE,
    {{ String_Transform('AVG_ABANDON_TIME') }} as AVG_ABANDON_TIME,
    {{ String_Transform('AVG_BUSY_TIME') }} as AVG_BUSY_TIME,
    {{ String_Transform('AVG_HANDLING_TIME') }} as AVG_HANDLING_TIME,
    {{ String_Transform('AVG_PROCESSING_TIME') }} as AVG_PROCESSING_TIME,
    {{ String_Transform('AVG_WAIT_BEFORE_ACCEPTED_TIME') }} as AVG_WAIT_BEFORE_ACCEPTED_TIME,
    {{ String_Transform('AVG_WAIT_TIME') }} as AVG_WAIT_TIME,
    {{ String_Transform('AVG_WRAP_UP_TIME') }} as AVG_WRAP_UP_TIME,
    {{ String_Transform('BUSY_TIME') }} as BUSY_TIME,
    {{ Number_Transform('DIVERTED') }} as DIVERTED,
    {{ DateTime_Transform('END_TIME') }} as END_TIME,
    {{ Number_Transform('ENTERED') }} as ENTERED,
    {{ String_Transform('HANDLING_TIME') }} as HANDLING_TIME,
    {{ String_Transform('LONGEST_ABANDON_TIME') }} as LONGEST_ABANDON_TIME,
    {{ String_Transform('LONGEST_WAIT_BEFORE_ACCEPT_TIME') }} as LONGEST_WAIT_BEFORE_ACCEPT_TIME,
    {{ String_Transform('LONGEST_WAIT_TIME') }} as LONGEST_WAIT_TIME,
    {{ Number_Transform('NEW_IN_QUEUE') }} as NEW_IN_QUEUE,
    {{ String_Transform('PROCESSING_TIME') }} as PROCESSING_TIME,
    {{ String_Transform('QUEUE') }} as QUEUE,
    {{ Number_Transform('QUEUE_ID') }} as QUEUE_ID,
    {{ Number_Transform('SLA_PERCENTAGE') }} as SLA_PERCENTAGE,
    {{ DateTime_Transform('START_TIME') }} as START_TIME,
    {{ Number_Transform('TOTAL') }} as TOTAL,
    {{ Number_Transform('TOTAL_ABANDONED') }} as TOTAL_ABANDONED,
    {{ Number_Transform('TOTAL_ABANDONED_PERCENTAGE') }} as TOTAL_ABANDONED_PERCENTAGE,
    {{ String_Transform('TOTAL_ABANDON_TIME') }} as TOTAL_ABANDON_TIME,
    {{ Number_Transform('VOICEMAILS_LEFT') }} as VOICEMAILS_LEFT,
    {{ Number_Transform('WAITING_IN_QUEUE') }} as WAITING_IN_QUEUE,
    {{ String_Transform('WAITING_IN_QUEUE_TIME') }} as WAITING_IN_QUEUE_TIME,
    {{ String_Transform('WRAP_UP_TIME') }} as WRAP_UP_TIME
    from {{ source('AMA_DEV_BRNZ_8X8', 'QUEUES_SUMMARY') }}
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