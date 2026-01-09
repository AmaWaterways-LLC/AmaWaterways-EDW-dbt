{% macro log_poc_job_run() %}
    {% if env_var('DBT_RUN_UAT_SETUP', 'false') == 'true' %}

        {% do log("POC: Logging dbt job completion", info=True) %}

        {% set sql %}
            INSERT INTO PC_DBT_DB.POC_SCHEMA.POC_AUDIT
            VALUES (
                '{{ invocation_id }}',
                'DBT_JOB_COMPLETED',
                CURRENT_TIMESTAMP()
            );
        {% endset %}

        {% do run_query(sql) %}

    {% else %}
        {% do log("POC: Skipping post-run logging (DBT_RUN_UAT_SETUP != true)", info=True) %}
    {% endif %}
{% endmacro %}
