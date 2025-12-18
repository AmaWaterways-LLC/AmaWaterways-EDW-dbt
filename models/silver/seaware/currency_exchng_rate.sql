{# ================================================================
   CONFIG BLOCK – FULL REFRESH TABLE
   ================================================================ #}
{{
    config(
        materialized = 'table'
    )
}}

{# ================================================================
   SOURCE CTEs
   ================================================================ #}

WITH sw1_src AS (
    SELECT
        'SW1' AS DATA_SOURCE,
        {{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
        {{ transform_numeric('EXCHANGE_RATE') }} AS EXCHANGE_RATE,
        {{ transform_date('DATE_FROM') }} AS DATE_FROM,
        {{ transform_date('DATE_TO') }} AS DATE_TO,
        {{ transform_numeric('EXCHANGE_RATE_ID') }} AS EXCHANGE_RATE_ID,
        NULL AS INVERSE_RATE,
        NULL AS CURRENCY_CODE_TO,
        {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
        _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source(var('bronze_source_prefix') ~ '_SW1', 'CURRENCY_EXCHNG_RATE') }}
),

sw2_src AS (
    SELECT
        'SW2' AS DATA_SOURCE,
        {{ transform_string('CURRENCY_CODE') }} AS CURRENCY_CODE,
        {{ transform_numeric('EXCHANGE_RATE') }} AS EXCHANGE_RATE,
        {{ transform_date('DATE_FROM') }} AS DATE_FROM,
        {{ transform_date('DATE_TO') }} AS DATE_TO,
        {{ transform_numeric('EXCHANGE_RATE_ID') }} AS EXCHANGE_RATE_ID,
        {{ transform_numeric('INVERSE_RATE') }} AS INVERSE_RATE,
        {{ transform_string('CURRENCY_CODE_TO') }} AS CURRENCY_CODE_TO,
        {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
        _FIVETRAN_DELETED AS SOURCE_DELETED
    FROM {{ source(var('bronze_source_prefix') ~ '_SW2', 'CURRENCY_EXCHNG_RATE') }}
)

{# ================================================================
   FINAL SELECT
   ================================================================ #}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'CURRENCY_CODE',
        'DATE_FROM',
        'DATA_SOURCE'
    ]) }} AS CURRENCY_EXCHNG_RATE_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'CURRENCY_CODE',
        'DATE_FROM',
        'DATA_SOURCE'
    ]) }} AS CURRENCY_EXCHNG_RATE_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src


{#
SELECT *
FROM (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'CURRENCY_CODE',
            'DATE_FROM',
            'DATA_SOURCE'
        ]) }} AS CURRENCY_EXCHNG_RATE_SURROGATE_KEY,
        sw1_src.*
    FROM sw1_src

    UNION ALL

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'CURRENCY_CODE',
            'DATE_FROM',
            'DATA_SOURCE'
        ]) }} AS CURRENCY_EXCHNG_RATE_SURROGATE_KEY,
        sw2_src.*
    FROM sw2_src
)
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY
            CURRENCY_CODE,
            DATE_FROM,
            DATA_SOURCE
        ORDER BY LAST_UPDATED_TIMESTAMP DESC
) = 1
#}