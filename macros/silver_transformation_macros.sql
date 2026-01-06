{# 
================================================================================
DBT MACROS FOR DATA QUALITY TRANSFORMATIONS
================================================================================
Corresponding SQL implementations for transformation_functions.py
#}

{# ============================================================================
   STRING TRANSFORM MACRO
   ============================================================================ #}

{% macro String_Transform(
    column_name,
    apply_trim=true,
    apply_whitespace_normalization=true,
    case_standard=none,
    convert_empty_to_null=true,
    default_value=none,
    min_length=none,
    max_length=none,
    email_validation=false,
    phone_validation=false,
    url_validation=false,
    remove_test_data=false,
    custom_mappings=none
) %}
{#
    Comprehensive string transformation macro
    Priority order:
    1. Trim → 2. Whitespace → 3. Case → 4. Empty to NULL → 5. Default
    6. Length → 7. Validation → 8. Mappings → 9. Test removal
#}

{%- set result = column_name -%}

{# 1. TRIM whitespace (Priority: High) #}
{%- if apply_trim -%}
    {%- set result = "TRIM(" ~ result ~ ")" -%}
{%- endif -%}




{# 2. Normalize internal whitespace (Priority: Medium) #}
{%- if apply_whitespace_normalization -%}
    {%- set result = "REGEXP_REPLACE(" ~ result ~ ", '[ \\t]+', ' ')" -%}
{%- endif -%}


{# 3. Case standardization (Priority: High/Medium) #}
{%- if case_standard == 'upper' -%}
    {%- set result = "UPPER(" ~ result ~ ")" -%}
{%- elif case_standard == 'lower' -%}
    {%- set result = "LOWER(" ~ result ~ ")" -%}
{%- elif case_standard == 'proper' -%}
    {%- set result = "INITCAP(" ~ result ~ ")" -%}
{%- endif -%}

{# 4. Convert empty to NULL (Priority: High) #}
{%- if convert_empty_to_null -%}
    {%- set result = "NULLIF(" ~ result ~ ", '')" -%}
{%- endif -%}

{# 5. Apply default values (Priority: High) #}
{%- if default_value -%}
    {%- set result = "COALESCE(" ~ result ~ ", '" ~ default_value ~ "')" -%}
{%- endif -%}

{# 6. Length validation (Priority: Medium) #}
{%- if min_length -%}
    {%- set result = "CASE WHEN LENGTH(" ~ result ~ ") < " ~ min_length ~ " THEN NULL ELSE " ~ result ~ " END" -%}
{%- endif -%}

{%- if max_length -%}
    {%- set result = "CASE WHEN LENGTH(" ~ result ~ ") > " ~ max_length ~ " THEN NULL ELSE " ~ result ~ " END" -%}
{%- endif -%}

{# 7. Email validation (Priority: High) #}
{%- if email_validation -%}
    {%- set result = "CASE WHEN REGEXP_LIKE(" ~ result ~ ", '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$') THEN " ~ result ~ " ELSE NULL END" -%}
{%- endif -%}

{# 8. Phone validation/cleaning (Priority: High) #}
{%- if phone_validation -%}
    {%- set result = "REGEXP_REPLACE(" ~ result ~ ", '[^0-9+]', '')" -%}
{%- endif -%}

{# 9. URL validation (Priority: Medium) #}
{%- if url_validation -%}
    {%- set result = "CASE WHEN LOWER(" ~ result ~ ") LIKE 'http%' THEN " ~ result ~ " ELSE NULL END" -%}
{%- endif -%}

{# 10. Custom mappings (Priority: High) #}
{%- if custom_mappings -%}
    {%- set result -%}
    CASE 
        {%- for key, value in custom_mappings.items() %}
        WHEN {{ result }} = '{{ key }}' THEN '{{ value }}'
        {%- endfor %}
        ELSE {{ result }}
    END
    {%- endset -%}
{%- endif -%}

{# 11. Remove test data (Priority: High) #}
{%- if remove_test_data -%}
    {%- set result = "CASE WHEN UPPER(" ~ result ~ ") IN ('TEST', 'DUMMY', 'SAMPLE', 'EXAMPLE') THEN NULL ELSE " ~ result ~ " END" -%}
{%- endif -%}

{{ result }}

{% endmacro %}


{# ============================================================================
   NUMERIC TRANSFORM MACRO
   ============================================================================ #}

{% macro Number_Transform(
    column_name,
    convert_null_to_zero=false,
    null_replacement_value=none,
    distinguish_null_from_zero=false,
    min_value=none,
    max_value=none,
    ensure_range=none,
    decimal_places=none,
    target_type='DECIMAL(18,2)',
    ensure_non_negative=false,
    detect_outliers=false,
    outlier_std_threshold=3.0
) %}
{#
    Comprehensive numeric transformation macro
    Priority order:
    1. Type casting → 2. NULL handling → 3. Range → 4. Precision
    5. Sign validation → 6. Outliers
#}

{# Always wrap the base expression #}
{%- set result = column_name | trim -%}

{# 1. Type casting #}
{%- set result = "CAST((" ~ result ~ ") AS " ~ target_type ~ ")" -%}

{# 2. NULL handling #}
{%- if convert_null_to_zero -%}
    {%- set result = "COALESCE((" ~ result ~ "), 0)" -%}
{%- elif null_replacement_value is not none -%}
    {%- set result = "COALESCE((" ~ result ~ "), " ~ null_replacement_value ~ ")" -%}
{%- elif distinguish_null_from_zero -%}
    {%- set result = "CASE WHEN (" ~ column_name ~ ") IS NULL THEN -1 ELSE (" ~ result ~ ") END" -%}
{%- endif -%}

{# 3. Range validation: minimum #}
{%- if min_value is not none -%}
    {%- set result = "CASE WHEN (" ~ result ~ ") < " ~ min_value ~ " THEN " ~ min_value ~ " ELSE (" ~ result ~ ") END" -%}
{%- endif -%}

{# 4. Range validation: maximum #}
{%- if max_value is not none -%}
    {%- set result = "CASE WHEN (" ~ result ~ ") > " ~ max_value ~ " THEN NULL ELSE (" ~ result ~ ") END" -%}
{%- endif -%}

{# 5. Range validation: between #}
{%- if ensure_range is not none -%}
    {%- set range_min = ensure_range[0] -%}
    {%- set range_max = ensure_range[1] -%}
    {%- set result = "CASE WHEN (" ~ result ~ ") BETWEEN " ~ range_min ~ " AND " ~ range_max ~ " THEN (" ~ result ~ ") ELSE NULL END" -%}
{%- endif -%}

{# 6. Precision rounding #}
{%- if decimal_places is not none -%}
    {%- set result = "ROUND((" ~ result ~ "), " ~ decimal_places ~ ")" -%}
{%- endif -%}

{# 7. Sign validation #}
{%- if ensure_non_negative -%}
    {%- set result = "CASE WHEN (" ~ result ~ ") < 0 THEN NULL ELSE (" ~ result ~ ") END" -%}
{%- endif -%}

{# 8. Outlier detection (kept commented exactly as you had it) #}
{#
{%- if detect_outliers -%}
    {%- set result -%}
    CASE 
        WHEN ABS({{ result }} - AVG({{ result }}) OVER ()) > ({{ outlier_std_threshold }} * STDDEV({{ result }}) OVER ())
        THEN NULL 
        ELSE {{ result }}
    END
    {%- endset -%}
{%- endif -%}
#}

{{ result }}

{% endmacro %}


{# ============================================================================
   DATE/TIMESTAMP TRANSFORM MACRO (ORIGINAL VERSION)
   ============================================================================ #}

{% macro DateTime_Transform(
    column_name,
    target_format='DATE',
    convert_to_utc=false,
    source_timezone=none,
    default_date=none,
    prevent_future_dates=false,
    min_year=1900,
    max_year=none,
    extract_component=none,
    calculate_age=false,
    generate_fiscal_quarter=false
) %}
{#
    Comprehensive date/timestamp transformation macro
    Priority order:
    1. Format → 2. Timezone → 3. NULL → 4. Future validation
    5. Range → 6. Calculations
#}

{%- set result = column_name -%}

{# 1. Format standardization (Priority: High) #}
{%- if target_format == 'DATE' -%}
    {%- set result = result ~ "::DATE" -%}
{%- elif target_format == 'TIMESTAMP' -%}
    {%- set result = result ~ "::TIMESTAMP" -%}
{%- endif -%}

{# 2. Timezone conversion (Priority: High) #}
{%- if convert_to_utc -%}
    {%- if source_timezone -%}
        {%- set result = "CONVERT_TIMEZONE('" ~ source_timezone ~ "', 'UTC', " ~ result ~ ")::TIMESTAMP" -%}
    {%- else -%}
        {%- set result = "CONVERT_TIMEZONE('UTC', " ~ result ~ ")::TIMESTAMP" -%}
    {%- endif -%}
{%- endif -%}

{# 3. NULL handling (Priority: Medium) #}
{%- if default_date -%}
    {%- if default_date == 'current' -%}
        {%- set result = "COALESCE(" ~ result ~ ", CURRENT_DATE())" -%}
    {%- else -%}
        {%- set result = "COALESCE(" ~ result ~ ", '" ~ default_date ~ "'::DATE)" -%}
    {%- endif -%}
{%- endif -%}

{# 4. Future date validation (Priority: High) #}
{%- if prevent_future_dates -%}
    {%- set result = "CASE WHEN " ~ result ~ " > CURRENT_DATE() THEN NULL ELSE " ~ result ~ " END" -%}
{%- endif -%}

{# 5. Historical validation (Priority: Medium) #}
{%- if min_year -%}
    {%- set result = "CASE WHEN YEAR(" ~ result ~ ") < " ~ min_year ~ " THEN NULL ELSE " ~ result ~ " END" -%}
{%- endif -%}

{%- if max_year -%}
    {%- set result = "CASE WHEN YEAR(" ~ result ~ ") > " ~ max_year ~ " THEN NULL ELSE " ~ result ~ " END" -%}
{%- endif -%}

{# 6. Date calculations and extractions #}
{%- if calculate_age -%}
    {%- set result = "DATEDIFF(YEAR, " ~ result ~ ", CURRENT_DATE())" -%}
{%- elif generate_fiscal_quarter -%}
    {%- set result = "'Q' || CEIL(MONTH(" ~ result ~ ")/3.0) || ' ' || YEAR(" ~ result ~ ")" -%}
{%- elif extract_component -%}
    {%- if extract_component == 'year' -%}
        {%- set result = "EXTRACT(YEAR FROM " ~ result ~ ")" -%}
    {%- elif extract_component == 'month' -%}
        {%- set result = "EXTRACT(MONTH FROM " ~ result ~ ")" -%}
    {%- elif extract_component == 'day' -%}
        {%- set result = "EXTRACT(DAY FROM " ~ result ~ ")" -%}
    {%- elif extract_component == 'quarter' -%}
        {%- set result = "EXTRACT(QUARTER FROM " ~ result ~ ")" -%}
    {%- endif -%}
{%- endif -%}

{{ result }}

{% endmacro %}


{# ============================================================================
   BOOLEAN TRANSFORM MACRO
   ============================================================================ #}

{% macro Boolean_Transform(
    column_name,
    convert_null_to_false=true,
    true_values=['Y', 'YES', '1', 'T', 'TRUE'],
    false_values=['N', 'NO', '0', 'F', 'FALSE']
) %}
{#
    Comprehensive boolean transformation macro
    Priority order:
    1. Standardization → 2. Type casting → 3. NULL handling
#}

{%- set result -%}
CASE 
    WHEN UPPER(TRIM({{ column_name }})) IN (
        {%- for val in true_values -%}
        '{{ val | upper }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
    ) THEN TRUE
    WHEN UPPER(TRIM({{ column_name }})) IN (
        {%- for val in false_values -%}
        '{{ val | upper }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
    ) THEN FALSE
    {%- if convert_null_to_false %}
    ELSE FALSE
    {%- else %}
    ELSE NULL
    {%- endif %}
END
{%- endset -%}

{{ result }}

{% endmacro %}


{# ============================================================================
   SAFE DIVISION MACRO (Utility)
   ============================================================================ #}

{% macro safe_divide(numerator, denominator, default_value=0) %}
{#
    Prevents division by zero errors
    Used in numeric transformations
#}

{{ numerator }} / NULLIF({{ denominator }}, 0)

{% endmacro %}


{# ============================================================================
   COMPREHENSIVE TRANSFORM MACRO (Orchestrator)
   ============================================================================ #}

{% macro apply_transform(column_name, transform_type, config={}) %}
{#
    Main orchestrator macro that routes to appropriate transformation
    
    Usage in dbt model:
    SELECT
        {{ apply_transform('email', 'string', {
            'case_standard': 'lower',
            'email_validation': true
        }) }} as email,
        {{ apply_transform('amount', 'numeric', {
            'decimal_places': 2,
            'ensure_non_negative': true
        }) }} as amount
    FROM source_table
#}

{%- if transform_type == 'string' or transform_type == 'String_Transform' -%}
    {{ string_transform(column_name, **config) }}

{%- elif transform_type == 'numeric' or transform_type == 'Numeric_Transform' -%}
    {{ numeric_transform(column_name, **config) }}

{%- elif transform_type == 'date' or transform_type == 'Date_Timestamp_Function' -%}
    {{ date_timestamp_transform(column_name, **config) }}

{%- elif transform_type == 'boolean' or transform_type == 'Boolean_Transform' -%}
    {{ boolean_transform(column_name, **config) }}

{%- else -%}
    {{ column_name }}  {# No transformation #}

{%- endif -%}

{% endmacro %}


{# ============================================================================
   GENERATE MODEL FROM CONTROL TABLE (Advanced)
   ============================================================================ #}

{% macro generate_silver_columns_from_control() %}
{#
    Generates SELECT clause columns based on control table configuration
    This would be used in conjunction with a Python script that reads the control table
    
    Example usage in model:
    {{ config(materialized='table') }}
    SELECT
        {{ generate_silver_columns_from_control() }}
    FROM {{ ref('bronze_table') }}
#}

{# This macro would typically be generated dynamically by your Python script #}
{# The Python script reads DBT_CONTROL_TABLE_TESTING and generates this #}

{% endmacro %}


{# ============================================================================
   EXAMPLE USAGE IN DBT MODEL
   ============================================================================ #}

{#

-- models/silver/customers_silver.sql

{{ config(
    materialized='incremental',
    unique_key='customer_id',
    tags=['silver', 'data_quality']
) }}

WITH source AS (
    SELECT * FROM {{ ref('customers_bronze') }}
),

transformed AS (
    SELECT
        customer_id,
        
        -- String transformations
        {{ string_transform(
            'email',
            case_standard='lower',
            email_validation=true,
            convert_empty_to_null=true,
            remove_test_data=true
        ) }} as email,
        
        {{ string_transform(
            'first_name',
            case_standard='proper',
            min_length=2,
            max_length=50
        ) }} as first_name,
        
        {{ string_transform(
            'phone',
            phone_validation=true
        ) }} as phone,
        
        -- Numeric transformations
        {{ numeric_transform(
            'account_balance',
            decimal_places=2,
            ensure_non_negative=true,
            max_value=999999999
        ) }} as account_balance,
        
        {{ numeric_transform(
            'credit_limit',
            convert_null_to_zero=false,
            decimal_places=2
        ) }} as credit_limit,
        
        -- Date transformations
        {{ date_timestamp_transform(
            'created_date',
            target_format='DATE',
            prevent_future_dates=true,
            min_year=1900
        ) }} as created_date,
        
        {{ date_timestamp_transform(
            'last_login',
            target_format='TIMESTAMP',
            convert_to_utc=true,
            source_timezone='America/New_York'
        ) }} as last_login_utc,
        
        -- Boolean transformations
        {{ boolean_transform(
            'is_active',
            convert_null_to_false=true
        ) }} as is_active,
        
        {{ boolean_transform(
            'email_opt_in'
        ) }} as email_opt_in,
        
        -- Metadata
        CURRENT_TIMESTAMP() as dbt_updated_at
        
    FROM source
    
    {% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(dbt_updated_at) FROM {{ this }})
    {% endif %}
)

SELECT * FROM transformed

#}




