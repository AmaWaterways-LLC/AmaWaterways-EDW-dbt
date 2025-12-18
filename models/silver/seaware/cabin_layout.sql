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
        NULL AS RECORD_ID,
        NULL AS CABIN_LAYOUT_HEADER_ID,
        {{ transform_numeric('CABIN_ID') }} AS CABIN_ID,
        {{ transform_string('OBJECT') }} AS OBJECT,
        {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
        _FIVETRAN_DELETED AS SOURCE_DELETED,
        {{ transform_numeric('CABIN_LAYOUT_ID') }} AS CABIN_LAYOUT_ID,
        {{ transform_numeric('DECK_ID') }} AS DECK_ID
    FROM {{ source(var('bronze_source_prefix') ~ '_SW1', 'CABIN_LAYOUT') }}
),

sw2_src AS (
    SELECT
        'SW2' AS DATA_SOURCE,
        {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
        {{ transform_numeric('CABIN_LAYOUT_HEADER_ID') }} AS CABIN_LAYOUT_HEADER_ID,
        {{ transform_numeric('CABIN_ID') }} AS CABIN_ID,
        {{ transform_string('OBJECT') }} AS OBJECT,
        {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
        _FIVETRAN_DELETED AS SOURCE_DELETED,
        NULL AS CABIN_LAYOUT_ID,
        NULL AS DECK_ID
    FROM {{ source(var('bronze_source_prefix') ~ '_SW2', 'CABIN_LAYOUT') }}
)

{# ================================================================
   FINAL SELECT
   ================================================================ #}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'DATA_SOURCE',
        'COALESCE(CABIN_LAYOUT_ID, RECORD_ID)'
    ]) }} AS CABIN_LAYOUT_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'DATA_SOURCE',
        'COALESCE(CABIN_LAYOUT_ID, RECORD_ID)'
    ]) }} AS CABIN_LAYOUT_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src


{#
SELECT *
FROM (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'DATA_SOURCE',
            'COALESCE(CABIN_LAYOUT_ID, RECORD_ID)'
        ]) }} AS CABIN_LAYOUT_SURROGATE_KEY,
        sw1_src.*
    FROM sw1_src

    UNION ALL

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'DATA_SOURCE',
            'COALESCE(CABIN_LAYOUT_ID, RECORD_ID)'
        ]) }} AS CABIN_LAYOUT_SURROGATE_KEY,
        sw2_src.*
    FROM sw2_src
)
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY
            DATA_SOURCE,
            COALESCE(CABIN_LAYOUT_ID, RECORD_ID)
        ORDER BY LAST_UPDATED_TIMESTAMP DESC
) = 1
#}