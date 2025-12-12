{# ================================================================
   FULL REFRESH CONFIGURATION
   ================================================================ #}

{{ config(
    materialized = 'table',   -- always full refresh
    full_refresh = true       -- ensures table is rebuilt completely
) }}

{# ================================================================
   SOURCE CTE
   ================================================================ #}

WITH src AS (
    SELECT
        'SW1' AS DATA_SOURCE,
        {{ transform_numeric('TREE_NODE_ID') }} AS TREE_NODE_ID,
        {{ transform_numeric('TRANS_ID') }} AS TRANS_ID,
        {{ transform_string('ACTION') }} AS ACTION,
        {{ transform_string('IT_IS_PARM') }} AS IT_IS_PARM,
        {{ transform_string('FIELD_ID') }} AS FIELD_ID,
        {{ transform_string('FIELD_VALUE_OLD') }} AS FIELD_VALUE_OLD,
        {{ transform_string('FIELD_VALUE_NEW') }} AS FIELD_VALUE_NEW,
        _FIVETRAN_DELETED AS SOURCE_DELETED,
        {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'RES_HIST_VALUE') }}
)

{# ================================================================
   DEDUPLICATION LOGIC
   We dedupe using all columns, EXCEPT Fivetran audit columns
   ================================================================ #}

, dedupe AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    TREE_NODE_ID,
                    TRANS_ID,
                    ACTION,
                    IT_IS_PARM,
                    FIELD_ID,
                    FIELD_VALUE_OLD,
                    FIELD_VALUE_NEW,
                    DATA_SOURCE
                ORDER BY LAST_UPDATED_TIMESTAMP DESC
            ) AS rn
        FROM src
    )
    WHERE rn = 1
)

{# ================================================================
   FINAL OUTPUT
   Generate a surrogate key for table compatibility
   ================================================================ #}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "TREE_NODE_ID",
        "TRANS_ID",
        "ACTION",
        "IT_IS_PARM",
        "FIELD_ID",
        "FIELD_VALUE_OLD",
        "FIELD_VALUE_NEW",
        "DATA_SOURCE"
    ]) }} AS RES_HIST_VALUE_SURROGATE_KEY,
    *
FROM dedupe
