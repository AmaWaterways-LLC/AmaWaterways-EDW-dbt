-- macros/unified_data_transformations.sql

{# ============================================================
   STRING TRANSFORMATIONS
   selector:
     1 = string_trim
     --2 = string_trim_with_na_fallback
   ============================================================ #}

{% macro transform_string(column_name) %}
    {#% if selector == 1 %#}
        TRIM({{ column_name }})

    {#% elif selector == 2 %}
        COALESCE(TRIM({{ column_name }}), 'NA')

    {% else %}
        {% do exceptions.raise_compiler_error("Unknown string selector: " ~ selector) %}
    {% endif %#}
{% endmacro %}


{# ============================================================
   NUMERIC TRANSFORMATIONS
   selector:
     1 = numeric_trim_digits
   ============================================================ #}

{% macro transform_numeric(column_name) %}
    {#% if selector == 1 %#}
        CASE 
            WHEN {{ column_name }} IS NULL THEN NULL
            ELSE REGEXP_REPLACE({{ column_name }}::VARCHAR, '[^0-9]', '')::NUMBER
        END

    {#% else %}
        {% do exceptions.raise_compiler_error("Unknown numeric selector: " ~ selector) %}
    {% endif %#}
{% endmacro %}



{# ============================================================
   DATE/TIMESTAMP TRANSFORMATIONS
   selector:
     1 = date_standardize_utc
     --2 = date_standardize_et
   ============================================================ #}

{% macro transform_datetime(column_name, is_join_key=false) %}
    {#% if selector == 1 %#}
        CASE 
            WHEN {{ column_name }} IS NULL THEN 
                {% if is_join_key %}
                    TRY_TO_TIMESTAMP_TZ('1900-01-01 00:00:000 +00:00))', 'YYYY-MM-DD HH24:MI:SS.FF TZHTZM')
                {% else %}
                    NULL
                {% endif %}
            WHEN TRY_TO_TIMESTAMP_TZ({{ column_name }}::VARCHAR, 'YYYY-MM-DD HH24:MI:SS.FF TZHTZM') IS NOT NULL THEN 
                TRY_TO_TIMESTAMP_TZ({{ column_name }}::VARCHAR, 'YYYY-MM-DD HH24:MI:SS.FF TZHTZM')
            ELSE 
                CONVERT_TIMEZONE('UTC', {{ column_name }}::TIMESTAMP_TZ(9))::TIMESTAMP_TZ(9)
        END

    {#% elif selector == 2 %}
        CASE 
            WHEN {{ column_name }} IS NULL THEN 
                {% if is_join_key %}
                    TO_DATE('1900-01-01', 'YYYY-MM-DD')
                {% else %}
                    NULL
                {% endif %}
            WHEN TRY_TO_DATE({{ column_name }}::VARCHAR, 'YYYY-MM-DD') IS NOT NULL THEN 
                TO_DATE({{ column_name }}::VARCHAR, 'YYYY-MM-DD')
            ELSE 
                CONVERT_TIMEZONE('America/New_York', {{ column_name }}::TIMESTAMP_NTZ)::DATE
        END

    {% else %}
        {% do exceptions.raise_compiler_error("Unknown datetime selector: " ~ selector) %}
    {% endif %#}
{% endmacro %}


-- macros/transform_date.sql

{% macro transform_date(column_name, is_join_key=false) %}
    CASE
        WHEN {{ column_name }} IS NULL THEN
            {% if is_join_key %}
                TO_DATE('1900-01-01', 'YYYY-MM-DD')
            {% else %}
                NULL
            {% endif %}

        WHEN TRY_TO_DATE({{ column_name }}::VARCHAR, 'YYYY-MM-DD') IS NOT NULL THEN
            TO_DATE({{ column_name }}::VARCHAR, 'YYYY-MM-DD')

        WHEN TRY_TO_DATE({{ column_name }}::VARCHAR, 'YYYY/MM/DD') IS NOT NULL THEN
            TO_DATE({{ column_name }}::VARCHAR, 'YYYY/MM/DD')

        ELSE
            TRY_TO_DATE({{ column_name }}::VARCHAR)
    END
{% endmacro %}


-- macros/transform_time.sql

{% macro transform_time(column_name, is_join_key=false) %}
    CASE
        WHEN {{ column_name }} IS NULL THEN
            {% if is_join_key %}
                TO_TIME('00:00:00', 'HH24:MI:SS')
            {% else %}
                NULL
            {% endif %}

        WHEN TRY_TO_TIME({{ column_name }}::VARCHAR, 'HH24:MI:SS') IS NOT NULL THEN
            TO_TIME({{ column_name }}::VARCHAR, 'HH24:MI:SS')

        WHEN TRY_TO_TIME({{ column_name }}::VARCHAR, 'HH24:MI') IS NOT NULL THEN
            TO_TIME({{ column_name }}::VARCHAR, 'HH24:MI')

        ELSE
            TRY_TO_TIME({{ column_name }}::VARCHAR)
    END
{% endmacro %}


-- macros/transform_boolean.sql

{% macro transform_boolean(column_name, is_join_key=false) %}
    CASE
        WHEN {{ column_name }} IS NULL THEN
            {% if is_join_key %}
                FALSE
            {% else %}
                NULL
            {% endif %}

        WHEN UPPER({{ column_name }}::VARCHAR) IN ('TRUE', 'T', 'YES', 'Y', '1') THEN TRUE
        WHEN UPPER({{ column_name }}::VARCHAR) IN ('FALSE', 'F', 'NO', 'N', '0') THEN FALSE

        ELSE NULL
    END
{% endmacro %}


