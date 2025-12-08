{# =========================
   Config & Watermark Utils
   ========================= #}

{% set LOG_DB = "AMA_RAW" %}
{% set LOG_SCHEMA = "ORACLE_V2_LOG_TABLES_TESTING_SEAWARE" %}
{% set CONFIG_TABLE = "TEST_CONFIG_TABLE" %}

{# --- Helper: emit safe SQL string literals (single-quoted, escaped) 
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
--- #}

{# --- Helper: robust, fully-qualified config table (works across envs) 
{% macro log_config_fqn() -%}
  {% set _db = var('config_database', (LOG_DB if (LOG_DB is defined and LOG_DB is not none) else target.database)) %}
  {% set _sc = var('config_schema', (LOG_SCHEMA if (LOG_SCHEMA is defined and LOG_SCHEMA is not none) else target.schema)) %}
  {% set _tb = var('config_table', (CONFIG_TABLE if (CONFIG_TABLE is defined and CONFIG_TABLE is not none) else 'TEST_CONFIG_TABLE')) %}
  {{ return(api.Relation.create(database=_db|string, schema=_sc|string, identifier=_tb|string)) }}
{%- endmacro %} --- #}

{# --- Helper: default watermark literal for coalesce operations --- #}
{% macro wm_default_literal() -%}
  '1900-01-01 00:00:00:000 +0000'::timestamp_tz
{%- endmacro %}

{# --- Helper: format watermark value for SQL comparison --- #}
{% macro _format_watermark(val) -%}
  {%- if val is none -%}
    null
  {%- elif val is string -%}
    {{ "'" ~ (val|string|replace("'", "''")) ~ "'" }}::timestamp_tz(9)
  {%- else -%}
    '{{ val }}'::timestamp_tz(9)
  {%- endif -%}
{%- endmacro %}

{# Fetch the config row for a given object. Returns a dict-like row. #}
{% macro get_config_row(data_source, database_name, schema_name, table_name) %}

  {% if not execute %}
    {# At compile time, just return a dummy row to avoid None errors #}
    {% do log("Skipping config lookup at compile time for " ~ data_source ~ "." ~ schema_name ~ "." ~ table_name, info=True) %}
    {{ return({
      'CONFIG_ID': None,
      'DATA_SOURCE': data_source,
      'DATABASE_NAME': database_name,
      'SCHEMA_NAME': schema_name,
      'TABLE_NAME': table_name,
      'LOAD_TYPE': None,
      'WATERMARK_COLUMN': None,
      'LAST_UPDATED_WATERMARK_VALUE': None
    }) }}
  {% endif %}

  {# Runtime logic #}
  {% set sql %}
    select
      config_id,
      data_source,
      database_name,
      schema_name,
      table_name,
      load_type,
      watermark_column,
      last_updated_watermark_value
    from AMA_RAW.ORACLE_V2_LOG_TABLES_TESTING_SEAWARE.TEST_CONFIG_TABLE
    where upper(trim(data_source))   = upper(trim({{ _audit_lit(data_source) }}))
      and upper(trim(database_name)) = upper(trim({{ _audit_lit(database_name) }}))
      and upper(trim(schema_name))   = upper(trim({{ _audit_lit(schema_name) }}))
      and upper(trim(table_name))    = upper(trim({{ _audit_lit(table_name) }}))
    qualify row_number() over (order by config_id desc) = 1
  {% endset %}

  {% if var('config_lookup_debug', false) %}
    {% do log("Executing SQL for get_config_row:\n" ~ sql, info=True) %}
  {% endif %}

  {% set res = run_query(sql) %}

  {% if execute and res and res.rows|length > 0 %}
    {% set row = res.rows[0] %}
    {{ return({
      'CONFIG_ID': row[0],
      'DATA_SOURCE': row[1],
      'DATABASE_NAME': row[2],
      'SCHEMA_NAME': row[3],
      'TABLE_NAME': row[4],
      'LOAD_TYPE': row[5],
      'WATERMARK_COLUMN': row[6],
      'LAST_UPDATED_WATERMARK_VALUE': row[7]
    }) }}
  {% else %}
    {% do exceptions.raise_compiler_error("Config row not found for " ~ data_source ~ "." ~ database_name ~ "." ~ schema_name ~ "." ~ table_name) %}
  {% endif %}

{% endmacro %}

{# Convenience: get watermark column name from config #}
{% macro get_watermark_column(data_source, database_name, schema_name, table_name) %}
  {% set row = get_config_row(data_source, database_name, schema_name, table_name) %}
  {{ return(row['WATERMARK_COLUMN']) }}
{% endmacro %}

{# Convenience: get last watermark value from config #}
{% macro get_watermark_value(data_source, database_name, schema_name, table_name) %}
  {% set row = get_config_row(data_source, database_name, schema_name, table_name) %}
  {{ return(row['LAST_UPDATED_WATERMARK_VALUE']) }}
{% endmacro %}

{# Update the config watermark to a provided value #}
{% macro update_config_watermark(data_source, database_name, schema_name, table_name, new_watermark_value) %}
  {% set sql %}
    update AMA_RAW.ORACLE_V2_LOG_TABLES_TESTING_SEAWARE.TEST_CONFIG_TABLE
       set last_updated_watermark_value = {{ _format_watermark(new_watermark_value) }}
     where upper(trim(data_source))   = upper(trim({{ _audit_lit(data_source) }}))
       and upper(trim(database_name)) = upper(trim({{ _audit_lit(database_name) }}))
       and upper(trim(schema_name))   = upper(trim({{ _audit_lit(schema_name) }}))
       and upper(trim(table_name))    = upper(trim({{ _audit_lit(table_name) }}))
  {% endset %}
  {% do run_query(sql) %}
  {% do log("Updated watermark for " ~ data_source ~ "." ~ database_name ~ "." ~ schema_name ~ "." ~ table_name ~ " to " ~ new_watermark_value, info=true) %}
{% endmacro %}

{# Compute the max watermark from a relation; returns a scalar #}
{% macro compute_max_watermark(relation, watermark_column) %}
  {% if watermark_column is none %}
    {{ return(none) }}
  {% endif %}
  {% set sql %}
    select max({{ watermark_column }}) as max_wm
    from {{ relation }}
  {% endset %}
  {% set res = run_query(sql) %}
  {% if execute and res and res.rows|length > 0 %}
    {% set max_value = res.rows[0][0] %}
    {{ return(max_value) }}
  {% else %}
    {{ return(none) }}
  {% endif %}
{% endmacro %}

{# Helper: Get record count from a relation #}
{% macro get_record_count(relation) %}
  {% set sql %}
    select count(*) as cnt
    from {{ relation }}
  {% endset %}
  {% set res = run_query(sql) %}
  {% if execute and res and res.rows|length > 0 %}
    {{ return(res.rows[0][0]) }}
  {% else %}
    {{ return(0) }}
  {% endif %}
{% endmacro %}