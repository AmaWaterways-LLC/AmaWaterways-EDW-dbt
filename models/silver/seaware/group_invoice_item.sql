{# ================================================================
   Generate Batch ID (must be top-of-file, before config)
   ================================================================ #}
{% set batch_id = invocation_id ~ '-' ~ this.name ~ '-' ~ modules.datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S%fZ') %}

{# ================================================================
   CONFIG BLOCK
   ================================================================ #}
{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key=['RECORD_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'GROUP_INVOICE_ITEM') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'GROUP_INVOICE_ITEM') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'GROUP_INVOICE_ITEM', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'GROUP_INVOICE_ITEM') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'GROUP_INVOICE_ITEM', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'GROUP_INVOICE_ITEM') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'GROUP_INVOICE_ITEM') %}
    {% set wm_col_sw2 = cfg_sw2['WATERMARK_COLUMN'] %}
    {% set last_wm_sw2 = cfg_sw2['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw2 = (last_wm_sw2 is none) %}
{% else %}
    {% set wm_col_sw1 = none %}
    {% set last_wm_sw1 = none %}
    {% set is_full_sw1 = true %}
    {% set wm_col_sw2 = none %}
    {% set last_wm_sw2 = none %}
    {% set is_full_sw2 = true %}
{% endif %}

{# ================================================================
   SOURCE CTE
   ================================================================ #}

WITH sw1_src AS (
    SELECT
            'SW1' AS DATA_SOURCE,
            {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
            {{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
            {{ transform_numeric('REC_SEQN') }} AS REC_SEQN,
            {{ transform_string('INVOICE_ITEM_TYPE') }} AS INVOICE_ITEM_TYPE,
            {{ transform_string('INVOICE_ITEM_SUBTYPE') }} AS INVOICE_ITEM_SUBTYPE,
            {{ transform_numeric('QUANTITY') }} AS QUANTITY,
            {{ transform_datetime('EFF_DATE') }} AS EFF_DATE,
            NULL AS AMOUNT,
            NULL AS TOTAL_AMOUNT,
            {{ transform_string('INVOICE_ITEM_SUBTYPE2') }} AS INVOICE_ITEM_SUBTYPE2,
            {{ transform_string('INVOICE_ITEM_SUBTYPE3') }} AS INVOICE_ITEM_SUBTYPE3,
            {{ transform_string('IS_MANUAL_ADJUSTMENT') }} AS IS_MANUAL_ADJUSTMENT,
            {{ transform_string('PROMO_CODE') }} AS PROMO_CODE,
            {{ transform_numeric('GROUP_SHIP_REQ_ID') }} AS GROUP_SHIP_REQ_ID,
            {{ transform_numeric('GROUP_HOTEL_REQ_ID') }} AS GROUP_HOTEL_REQ_ID,
            {{ transform_numeric('GROUP_PACKAGE_ID') }} AS GROUP_PACKAGE_ID,
            {{ transform_numeric('PACKAGE_ID') }} AS PACKAGE_ID,
            {{ transform_numeric('COMMISSION_PERCENT') }} AS COMMISSION_PERCENT,
            {{ transform_numeric('DUMMY_RES_SEQN') }} AS DUMMY_RES_SEQN,
            {{ transform_numeric('GUEST_SEQN') }} AS GUEST_SEQN,
            NULL AS PRICE_AREA,
            NULL AS EXTENDED_INFO,
            NULL AS GROUP_AMENITY_REQ_ID,
            NULL AS GROUP_CARDECK_LEG_ID,
            NULL AS GROUP_DINING_REQ_ID,
            NULL AS GROUP_SHIP_ROOM_REQ_ID,
            NULL AS PERCENT,
            NULL AS N_OF_UNITS,
            NULL AS PRICE_PER_UNIT,
            NULL AS TOTAL_N_OF_UNITS,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_numeric('BROCHURE_PRICE') }} AS BROCHURE_PRICE,
            {{ transform_numeric('EBD_PRICE') }} AS EBD_PRICE,
            {{ transform_numeric('GROUP_PRICE') }} AS GROUP_PRICE,
            {{ transform_numeric('TOTAL_PRICE') }} AS TOTAL_PRICE,
            {{ transform_numeric('GROUP_AIR_REQ_ID') }} AS GROUP_AIR_REQ_ID
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'GROUP_INVOICE_ITEM') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
            {{ transform_numeric('GROUP_ID') }} AS GROUP_ID,
            {{ transform_numeric('REC_SEQN') }} AS REC_SEQN,
            {{ transform_string('INVOICE_ITEM_TYPE') }} AS INVOICE_ITEM_TYPE,
            {{ transform_string('INVOICE_ITEM_SUBTYPE') }} AS INVOICE_ITEM_SUBTYPE,
            {{ transform_numeric('QUANTITY') }} AS QUANTITY,
            {{ transform_datetime('EFF_DATE') }} AS EFF_DATE,
            {{ transform_numeric('AMOUNT') }} AS AMOUNT,
            {{ transform_numeric('TOTAL_AMOUNT') }} AS TOTAL_AMOUNT,
            {{ transform_string('INVOICE_ITEM_SUBTYPE2') }} AS INVOICE_ITEM_SUBTYPE2,
            {{ transform_string('INVOICE_ITEM_SUBTYPE3') }} AS INVOICE_ITEM_SUBTYPE3,
            {{ transform_string('IS_MANUAL_ADJUSTMENT') }} AS IS_MANUAL_ADJUSTMENT,
            {{ transform_string('PROMO_CODE') }} AS PROMO_CODE,
            {{ transform_numeric('GROUP_SHIP_REQ_ID') }} AS GROUP_SHIP_REQ_ID,
            {{ transform_numeric('GROUP_HOTEL_REQ_ID') }} AS GROUP_HOTEL_REQ_ID,
            {{ transform_numeric('GROUP_PACKAGE_ID') }} AS GROUP_PACKAGE_ID,
            {{ transform_numeric('PACKAGE_ID') }} AS PACKAGE_ID,
            {{ transform_numeric('COMMISSION_PERCENT') }} AS COMMISSION_PERCENT,
            {{ transform_numeric('DUMMY_RES_SEQN') }} AS DUMMY_RES_SEQN,
            {{ transform_numeric('GUEST_SEQN') }} AS GUEST_SEQN,
            {{ transform_string('PRICE_AREA') }} AS PRICE_AREA,
            {{ transform_string('EXTENDED_INFO') }} AS EXTENDED_INFO,
            {{ transform_numeric('GROUP_AMENITY_REQ_ID') }} AS GROUP_AMENITY_REQ_ID,
            {{ transform_numeric('GROUP_CARDECK_LEG_ID') }} AS GROUP_CARDECK_LEG_ID,
            {{ transform_numeric('GROUP_DINING_REQ_ID') }} AS GROUP_DINING_REQ_ID,
            {{ transform_numeric('GROUP_SHIP_ROOM_REQ_ID') }} AS GROUP_SHIP_ROOM_REQ_ID,
            {{ transform_numeric('PERCENT') }} AS PERCENT,
            {{ transform_numeric('N_OF_UNITS') }} AS N_OF_UNITS,
            {{ transform_numeric('PRICE_PER_UNIT') }} AS PRICE_PER_UNIT,
            {{ transform_numeric('TOTAL_N_OF_UNITS') }} AS TOTAL_N_OF_UNITS,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            NULL AS BROCHURE_PRICE,
            NULL AS EBD_PRICE,
            NULL AS GROUP_PRICE,
            NULL AS TOTAL_PRICE,
            NULL AS GROUP_AIR_REQ_ID
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'GROUP_INVOICE_ITEM') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["RECORD_ID", "DATA_SOURCE"]) }} AS GROUP_INVOICE_ITEM_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["RECORD_ID", "DATA_SOURCE"]) }} AS GROUP_INVOICE_ITEM_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

