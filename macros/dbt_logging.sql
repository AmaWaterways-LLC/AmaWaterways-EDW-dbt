{% macro get_dbt_logging_relation() %}
  {{ return(
      api.Relation.create(
        database   = var('dbt_logging_database', target.database),
        schema     = var('dbt_logging_schema'),
        identifier = var('dbt_logging_table')
      )
  ) }}
{% endmacro %}

{% macro dbt_logging(results, graph) -%}
{#
{% set audit_log_schema = "dbt_mkompelli" %}
{% set audit_log_table = "dbt_audit_log" %}
#}

{# Reset previous active flags #}
{#
{% do run_query(
    "UPDATE " ~ target.database ~ "." ~ audit_log_schema ~ "." ~ audit_log_table ~
    " SET active_flag = 0 WHERE active_flag = 1"
) %}
#}
{% set log_rel = get_dbt_logging_relation() %}
{% do run_query(
    "UPDATE " ~ log_rel ~
    " SET active_flag = 0 WHERE active_flag = 1"
) %}


{% for result in results if result.node.resource_type in ['model'] %}

        {% set model_name = result.node.name %}
        {% set status = result.status | upper %}

        {# DETECT MODEL LAYER BASED ON DIRECTORY #}
        {% if 'silver' in result.node.path %}
            {% set layer = 'SILVER' %}
        {% elif 'gold' in result.node.path %}
            {% set layer = 'GOLD' %}
        {% elif 'model' in result.node.path %}
            {% set layer = 'MODEL' %}
        {% else %}
            {% set layer = 'UNKNOWN' %}
        {% endif %}

        {% set upstream_sources = result.node.depends_on.sources %}
        {#% do log(result.node | list, info=True) %}
        {% do log(result.node | list, info=True) %#}
        {% set node_id = result.node.unique_id %}
        {% set graph = context['graph'] %}
        {% do log('graph results' ~ graph.nodes[node_id].sources, info=True) %}

        {% set node = result.node %}
        {% set sources = [] %}
        {% for dep in node.depends_on.nodes %}
            {% if dep.startswith('source.') %}
                {% set source = graph.sources.get(dep) %}
                {% if source %}
                    {% do sources.append(source.schema) %}
                    {% do log(
                        'Source: ' ~ source.database ~ '.' ~ source.schema ~ '.' ~ source.name,
                        info=True
                    ) %}
                {% else %}
                    {% do log('Could not resolve source: ' ~ dep, info=True) %}
                {% endif %}
            {% endif %}
        {% endfor %}

        {% set upstream_sources = sources | join(',') %}

        {% if upstream_sources | length > 0 %}
            {% if layer == 'SILVER' %}
                {#% set first_source = upstream_sources[0] %}
                {% set source_system = first_source.split('.')[1] | upper %#}
                {% set source_system = upstream_sources %}
            {% else %}
                {% set source_system = 'UNKNOWN' %}
            {% endif %}
        {% else %}
            {% set source_system = 'UNKNOWN' %}
        {% endif %}
         

        {# SAFE timing extraction #}
        {% set start_time = result.timing[0].started_at if result.timing | length > 0 else none %}
        {% set end_time = result.timing[1].completed_at if result.timing | length > 1 else none %}
        {% set duration = result.execution_time if result.execution_time is not none else 0 %}

        {% set rows_affected = result.adapter_response.get("rows_affected", 0) %}

        {# SAFE error escaping #}
        {% set error_message %}
            {% if status == 'SUCCESS' %}
                NULL
            {% else %}
                {{ ("'" ~ (result.message | replace("'", "''")) ~ "'") }}
            {% endif %}
        {% endset %}

        {# INSERT LOG #}
        {% set insert_sql %}
            {#INSERT INTO {{ target.database }}.{{ audit_log_schema }}.{{ audit_log_table }} (#}
            INSERT INTO {{ log_rel }} (
                run_id,
                run_time,
                model_name,
                model_layer,
                run_status,
                start_time,
                end_time,
                duration_seconds,
                rows_affected,
                error_message,
                active_flag,
                last_modified_date,
                source_layer
            )
            VALUES (
                '{{ invocation_id }}',
                '{{ run_started_at }}',
                '{{ model_name }}',
                '{{ layer }}',
                '{{ status }}',
                {% if start_time %}'{{ start_time }}'{% else %} NULL {% endif %},
                {% if end_time %}'{{ end_time }}'{% else %} NULL {% endif %},
                {{ duration }},
                {{ rows_affected }},
                {{ error_message }},
                1,
                current_timestamp(),
                '{{ source_system }}'
            )
        {% endset %}

        {% do run_query(insert_sql) %}

    {% endfor %}

{%- endmacro %}

