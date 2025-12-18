{# ====================
   Audit Logging Utils (Fully Self-contained FQN)
   ==================== #}
{#
{% set AUDIT_TABLE_FQN = api.Relation.create(
    database="AMA_RAW",
    schema="ORACLE_V2_LOG_TABLES_TESTING_SEAWARE",
    identifier="TEST_AUDIT_TABLE"
) %}
#}

{% macro get_audit_relation() %}
  {{ return(
      api.Relation.create(
        database   = var('audit_database'),
        schema     = var('audit_schema'),
        identifier = var('audit_table')
      )
  ) }}
{% endmacro %}


{# --- Helper: emit safe SQL literals (handles strings, numbers, booleans) --- #}
{% macro _audit_lit(val) -%}
  {%- if val is none -%}
    null
  {%- elif val is number -%}
    {{ val }}
  {%- elif val is boolean -%}
    {{ val }}
  {%- else -%}
    {{ "'" ~ (val|string|replace("'", "''")) ~ "'" }}
  {%- endif -%}
{%- endmacro %}


{# Insert a start row and return a batch_id (string) to correlate updates #}
{% macro audit_start(pipeline_name, source_name, database_name, schema_name, table_name, layer, operation_type, load_type, environment, ingested_by, batch_id) %}
    
    {# DEBUG: Log parameters if enabled #}
    {% if var('audit_debug', false) %}
        {% do log("=" * 80, info=true) %}
        {% do log("AUDIT_START CALLED", info=true) %}
        {% do log("  pipeline_name: " ~ pipeline_name, info=true) %}
        {% do log("  source_name: " ~ source_name, info=true) %}
        {% do log("  database_name: " ~ database_name, info=true) %}
        {% do log("  schema_name: " ~ schema_name, info=true) %}
        {% do log("  table_name: " ~ table_name, info=true) %}
        {% do log("  layer: " ~ layer, info=true) %}
        {% do log("  operation_type: " ~ operation_type, info=true) %}
        {% do log("  load_type: " ~ load_type, info=true) %}
        {% do log("  environment: " ~ environment, info=true) %}
        {% do log("  ingested_by: " ~ ingested_by, info=true) %}
        {% do log("  batch_id: " ~ batch_id, info=true) %}
        {#{% do log("  Audit table: " ~ AUDIT_TABLE_FQN, info=true) %}#}
        {% do log("  Audit table: " ~ get_audit_relation(), info=true) %}
        {% do log("=" * 80, info=true) %}
    {% endif %}

    {% set sql %}
        {% set audit_rel = get_audit_relation() %}
        insert into {{ audit_rel }} (
            pipeline_name, 
            source_name, 
            database_name, 
            schema_name, 
            table_name, 
            layer,
            operation_type, 
            load_type, 
            batch_id, 
            record_count_source, 
            record_count_target,
            start_time, 
            status, 
            environment, 
            ingested_by
        )
        values (
            {{ _audit_lit(pipeline_name) }},
            {{ _audit_lit(source_name) }},
            {{ _audit_lit(database_name) }},
            {{ _audit_lit(schema_name) }},
            {{ _audit_lit(table_name) }},
            {{ _audit_lit(layer) }},
            {{ _audit_lit(operation_type) }},
            {{ _audit_lit(load_type) }},
            {{ _audit_lit(batch_id) }},
            null,
            null,
            current_timestamp(),
            'STARTED',
            {{ _audit_lit(environment) }},
            {{ _audit_lit(ingested_by) }}
        )
    {% endset %}
    
    {# DEBUG: Show SQL if enabled #}
    {% if var('audit_debug', false) %}
        {% do log("AUDIT_START SQL:", info=true) %}
        {% do log(sql, info=true) %}
        {% do log("", info=true) %}
    {% endif %}
    
    {% do run_query(sql) %}
    
    {% if var('audit_debug', false) %}
        {% do log("✓ Audit start record inserted successfully", info=true) %}
    {% endif %}
    
{% endmacro %}

{# Update counts (source/target) #}
{% macro audit_update_counts(batch_id, record_count_source=None, record_count_target=None) %}
    
    {# DEBUG: Log parameters if enabled #}
    {% if var('audit_debug', false) %}
        {% do log("AUDIT_UPDATE_COUNTS called for batch_id: " ~ batch_id, info=true) %}
        {% do log("  record_count_source: " ~ record_count_source ~ " (type: " ~ (record_count_source.__class__.__name__ if record_count_source is not none else 'None') ~ ")", info=true) %}
        {% do log("  record_count_target: " ~ record_count_target ~ " (type: " ~ (record_count_target.__class__.__name__ if record_count_target is not none else 'None') ~ ")", info=true) %}
    {% endif %}
    
    {% set set_parts = [] %}
    {% if record_count_source is not none %}
        {% do set_parts.append("record_count_source = " ~ _audit_lit(record_count_source)) %}
    {% endif %}
    {% if record_count_target is not none %}
        {% do set_parts.append("record_count_target = " ~ _audit_lit(record_count_target)) %}
    {% endif %}
    
    {% if set_parts|length == 0 %}
        {% if var('audit_debug', false) %}
            {% do log("⚠ No counts to update, skipping", info=true) %}
        {% endif %}
        {{ return(None) }}
    {% endif %}
    
    {% set sql %}
        {% set audit_rel = get_audit_relation() %}
        update {{ audit_rel }}
           set {{ set_parts|join(', ') }}
         where batch_id = {{ _audit_lit(batch_id) }}
    {% endset %}
    
    {# DEBUG: Show SQL if enabled #}
    {% if var('audit_debug', false) %}
        {% do log("AUDIT_UPDATE_COUNTS SQL:", info=true) %}
        {% do log(sql, info=true) %}
    {% endif %}
    
    {% do run_query(sql) %}
    
    {% if var('audit_debug', false) %}
        {% do log("✓ Audit counts updated successfully", info=true) %}
    {% endif %}
{% endmacro %}

{# Mark completion (SUCCESS/FAILED/WARNING) #}
{% macro audit_end(batch_id, status='SUCCESS', error_message=None) %}
    
    {# DEBUG: Log parameters if enabled #}
    {% if var('audit_debug', false) %}
        {% do log("AUDIT_END called for batch_id: " ~ batch_id, info=true) %}
        {% do log("  status: " ~ status, info=true) %}
        {% do log("  error_message: " ~ (error_message if error_message else 'None'), info=true) %}
    {% endif %}
    
    {% set sql %}
        {% set audit_rel = get_audit_relation() %}
        update {{ audit_rel }}
           set end_time = current_timestamp(),
               duration_seconds = datediff('second', start_time, current_timestamp()),
               status = {{ _audit_lit(status) }},
               error_message = {{ _audit_lit(error_message) }}
         where batch_id = {{ _audit_lit(batch_id) }}
    {% endset %}
    
    {# DEBUG: Show SQL if enabled #}
    {% if var('audit_debug', false) %}
        {% do log("AUDIT_END SQL:", info=true) %}
        {% do log(sql, info=true) %}
    {% endif %}
    
    {% do run_query(sql) %}
    
    {% if var('audit_debug', false) %}
        {% do log("✓ Audit end record updated successfully", info=true) %}
    {% endif %}
{% endmacro %}

{# =========================
   DEBUG HELPER: Check Audit Table
   ========================= #}
{% macro debug_audit_table() %}
    {% do log("=" * 80, info=true) %}
    {% do log("AUDIT TABLE DEBUG INFO", info=true) %}
    {% do log("=" * 80, info=true) %}
    {% do log("Audit table location: " ~ AUDIT_TABLE_FQN, info=true) %}
    
    {# Check if table exists #}
    {% set check_sql %}
        select count(*) as cnt
        {% set audit_rel = get_audit_relation() %}
        from {{ audit_rel }}
        limit 1
    {% endset %}
    
    {% if execute %}
        {% set result = run_query(check_sql) %}
        {% if result and result.rows|length > 0 %}
            {% do log("✓ Audit table exists and is accessible", info=true) %}
            
            {# Show table structure #}
            {% set structure_sql %}
                select column_name, data_type
                {#from AMA_RAW.information_schema.columns
                where table_schema = 'ORACLE_V2_LOG_TABLES_TESTING_SEAWARE'
                  and table_name = 'TEST_AUDIT_TABLE'#}
                from {{ var('audit_database') }}.information_schema.columns
                where table_schema = {{ _audit_lit(var('audit_schema')) }}
                  and table_name   = {{ _audit_lit(var('audit_table')) }}
                order by ordinal_position
            {% endset %}
            
            {% set structure = run_query(structure_sql) %}
            {% if structure and structure.rows|length > 0 %}
                {% do log("", info=true) %}
                {% do log("Table structure:", info=true) %}
                {% do log("-" * 80, info=true) %}
                {% for row in structure.rows %}
                    {% do log("  " ~ row[0] ~ " (" ~ row[1] ~ ")", info=true) %}
                {% endfor %}
            {% endif %}
            
            {# Show recent records #}
            {% set recent_sql %}
                select 
                    batch_id,
                    pipeline_name,
                    table_name,
                    status,
                    start_time,
                    end_time
                {% set audit_rel = get_audit_relation() %}
                from {{ audit_rel }}
                order by start_time desc
                limit 5
            {% endset %}
            
            {% set recent = run_query(recent_sql) %}
            {% if recent and recent.rows|length > 0 %}
                {% do log("", info=true) %}
                {% do log("Recent audit records (5 most recent):", info=true) %}
                {% do log("-" * 80, info=true) %}
                {% for row in recent.rows %}
                    {% do log("  " ~ row[1] ~ "." ~ row[2] ~ " | Status: " ~ row[3] ~ " | Batch: " ~ row[0], info=true) %}
                {% endfor %}
            {% else %}
                {% do log("", info=true) %}
                {% do log("⚠ Audit table is EMPTY", info=true) %}
            {% endif %}
        {% else %}
            {% do log("✗ Audit table does NOT exist or is not accessible!", info=true) %}
        {% endif %}
    {% endif %}
    
    {% do log("=" * 80, info=true) %}
{% endmacro %}

{% macro audit_failure_handler(results) %}

    {% for r in results %}
        {% if r.node.resource_type != "model" %}
            {% continue %}
        {% endif %}

        {% if r.status == "success" %}
            {% continue %}
        {% endif %}

        {# Read batch_id from model meta #}
        {% set batch_id = r.node.config.meta.get('batch_id') %}

        {% if batch_id is none %}
            {{ log("⚠ No batch_id available for failed model: " ~ r.node.name, info=true) }}
            {% continue %}
        {% endif %}

        {{ log("⚠ Updating FAILED audit record for batch_id=" ~ batch_id, info=true) }}

        {% do audit_end(
            batch_id=batch_id,
            status='FAILED',
            error_message=r.message
        ) %}
    {% endfor %}

{% endmacro %}
