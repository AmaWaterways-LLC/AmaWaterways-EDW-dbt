{% macro generate_schema_name(custom_schema_name, node) %}
    {# 
      If a model specifies +schema, use it exactly.
      Otherwise, fall back to the target schema.
    #}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{% endmacro %}
