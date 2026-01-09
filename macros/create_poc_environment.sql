{% macro create_poc_environment() %}
    {% if env_var('DBT_RUN_UAT_SETUP', 'false') == 'true' %}

        {% do log("POC: Creating database, schema, and table", info=True) %}

        {% set sql %}
            CREATE DATABASE IF NOT EXISTS POC_DB;

            CREATE SCHEMA IF NOT EXISTS POC_DB.POC_SCHEMA;

            CREATE TABLE IF NOT EXISTS POC_DB.POC_SCHEMA.POC_AUDIT (
                run_id STRING,
                event STRING,
                event_ts TIMESTAMP
            );
        {% endset %}

        {% do run_query(sql) %}

    {% else %}
        {% do log("POC: Skipping environment setup (DBT_RUN_UAT_SETUP != true)", info=True) %}
    {% endif %}
{% endmacro %}
