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
        unique_key=['_FIVETRAN_ID', 'ALLOCATION_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'SHIP_INVENTORY_ALLOC') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_ship_inventory_alloc',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='SHIP_INVENTORY_ALLOC',
                     layer='SILVER',
                     operation_type='MERGE',
                     load_type=load_type_val,
                     environment=target.name,
                     ingested_by='dbt',
                     batch_id='" ~ batch_id ~ "'
                 ) %}              
             {% endif %}"
        ],
        post_hook=[
            "{% do audit_update_counts(batch_id='" ~ batch_id ~ "', record_count_target=get_record_count(this)) %}",
            "{% do audit_end(batch_id='" ~ batch_id ~ "', status='SUCCESS') %}",
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'SHIP_INVENTORY_ALLOC') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'SHIP_INVENTORY_ALLOC', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'SHIP_INVENTORY_ALLOC') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'SHIP_INVENTORY_ALLOC', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'SHIP_INVENTORY_ALLOC') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'SHIP_INVENTORY_ALLOC') %}
    {% set wm_col_sw2 = cfg_sw2['WATERMARK_COLUMN'] %}
    {% set last_wm_sw2 = cfg_sw2['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw2 = (last_wm is none) %}
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

with sw1_src as (
    select
    'SW1' AS DATA_SOURCE,
{{ transform_numeric('ALLOCATION_ID') }} AS ALLOCATION_ID,
{{ transform_string('ALLOCATION_OWNER_TYPE') }} AS ALLOCATION_OWNER_TYPE,
{{ transform_numeric('ALLOCATION_OWNER_ID') }} AS ALLOCATION_OWNER_ID,
{{ transform_string('OPERATOR_ID') }} AS OPERATOR_ID,
{{ transform_datetime('ALLOCATION_TIMESTAMP') }} AS ALLOCATION_TIMESTAMP,
{{ transform_numeric('PRIORITY') }} AS PRIORITY,
{{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
{{ transform_numeric('REL_DAY_FROM') }} AS REL_DAY_FROM,
{{ transform_numeric('REL_DAY_TO') }} AS REL_DAY_TO,
{{ transform_datetime('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
{{ transform_datetime('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
{{ transform_string('CABIN_CATEGORY') }} AS CABIN_CATEGORY,
{{ transform_numeric('DEPENDS_ON_ID') }} AS DEPENDS_ON_ID,
{{ transform_string('INVENTORY_REQUEST_TYPE') }} AS INVENTORY_REQUEST_TYPE,
{{ transform_string('INVENTORY_RESULT_TYPE') }} AS INVENTORY_RESULT_TYPE,
{{ transform_string('CABIN_NUMBER') }} AS CABIN_NUMBER,
{{ transform_numeric('PROBABILITY') }} AS PROBABILITY,
{{ transform_numeric('DECLINE_REASON') }} AS DECLINE_REASON,
{{ transform_datetime('RELEASE_TIMESTAMP') }} AS RELEASE_TIMESTAMP,
{{ transform_numeric('OCCUPANCY') }} AS OCCUPANCY,
{{ transform_string('PRICE_CATEGORY') }} AS PRICE_CATEGORY,
{{ transform_string('ROLLAWAY_BEDS_REQUESTED') }} AS ROLLAWAY_BEDS_REQUESTED,
{{ transform_string('RESERVE_TYPE') }} AS RESERVE_TYPE,
{{ transform_string('WTL_OWNER_FLAG') }} AS WTL_OWNER_FLAG,
{{ transform_numeric('DLGT_RES_ID') }} AS DLGT_RES_ID,
{{ transform_string('IS_DLGT_OBJ') }} AS IS_DLGT_OBJ,
NULL AS OVERLAY_DAY_FROM,
NULL AS OVERLAY_DAY_TO,
NULL AS DEP_REF_ID,
NULL AS ARR_REF_ID,
NULL AS OVERLAY_REF_FROM,
NULL AS OVERLAY_REF_TO,
{{ transform_string('IS_SOFT_ASSIGN') }} AS IS_SOFT_ASSIGN,
NULL AS CHILD_OCCUPANCY,
NULL AS MAX_CABIN_OCCUPANCY,
NULL AS GENDER,
NULL AS BERTH_NUMBER,
NULL AS FZ_ADJUSTMENT,
NULL AS ALLOTMENT_CABIN_ID,
NULL AS ALLOTMENT_AGREEMENT_ID,
NULL AS PAX_RESERVE_TYPE,
NULL AS PAX_TYPE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW1', 'SHIP_INVENTORY_ALLOC') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('ALLOCATION_ID') }} AS ALLOCATION_ID,
{{ transform_string('ALLOCATION_OWNER_TYPE') }} AS ALLOCATION_OWNER_TYPE,
{{ transform_numeric('ALLOCATION_OWNER_ID') }} AS ALLOCATION_OWNER_ID,
{{ transform_string('OPERATOR_ID') }} AS OPERATOR_ID,
{{ transform_datetime('ALLOCATION_TIMESTAMP') }} AS ALLOCATION_TIMESTAMP,
{{ transform_numeric('PRIORITY') }} AS PRIORITY,
{{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
{{ transform_numeric('REL_DAY_FROM') }} AS REL_DAY_FROM,
{{ transform_numeric('REL_DAY_TO') }} AS REL_DAY_TO,
{{ transform_datetime('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
{{ transform_datetime('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
{{ transform_string('CABIN_CATEGORY') }} AS CABIN_CATEGORY,
{{ transform_numeric('DEPENDS_ON_ID') }} AS DEPENDS_ON_ID,
{{ transform_string('INVENTORY_REQUEST_TYPE') }} AS INVENTORY_REQUEST_TYPE,
{{ transform_string('INVENTORY_RESULT_TYPE') }} AS INVENTORY_RESULT_TYPE,
{{ transform_string('CABIN_NUMBER') }} AS CABIN_NUMBER,
{{ transform_numeric('PROBABILITY') }} AS PROBABILITY,
{{ transform_numeric('DECLINE_REASON') }} AS DECLINE_REASON,
{{ transform_datetime('RELEASE_TIMESTAMP') }} AS RELEASE_TIMESTAMP,
{{ transform_numeric('OCCUPANCY') }} AS OCCUPANCY,
{{ transform_string('PRICE_CATEGORY') }} AS PRICE_CATEGORY,
{{ transform_string('ROLLAWAY_BEDS_REQUESTED') }} AS ROLLAWAY_BEDS_REQUESTED,
{{ transform_string('RESERVE_TYPE') }} AS RESERVE_TYPE,
{{ transform_string('WTL_OWNER_FLAG') }} AS WTL_OWNER_FLAG,
{{ transform_numeric('DLGT_RES_ID') }} AS DLGT_RES_ID,
{{ transform_string('IS_DLGT_OBJ') }} AS IS_DLGT_OBJ,
{{ transform_numeric('OVERLAY_DAY_FROM') }} AS OVERLAY_DAY_FROM,
{{ transform_numeric('OVERLAY_DAY_TO') }} AS OVERLAY_DAY_TO,
{{ transform_numeric('DEP_REF_ID') }} AS DEP_REF_ID,
{{ transform_numeric('ARR_REF_ID') }} AS ARR_REF_ID,
{{ transform_numeric('OVERLAY_REF_FROM') }} AS OVERLAY_REF_FROM,
{{ transform_numeric('OVERLAY_REF_TO') }} AS OVERLAY_REF_TO,
{{ transform_string('IS_SOFT_ASSIGN') }} AS IS_SOFT_ASSIGN,
{{ transform_numeric('CHILD_OCCUPANCY') }} AS CHILD_OCCUPANCY,
{{ transform_numeric('MAX_CABIN_OCCUPANCY') }} AS MAX_CABIN_OCCUPANCY,
{{ transform_string('GENDER') }} AS GENDER,
{{ transform_numeric('BERTH_NUMBER') }} AS BERTH_NUMBER,
{{ transform_numeric('FZ_ADJUSTMENT') }} AS FZ_ADJUSTMENT,
{{ transform_numeric('ALLOTMENT_CABIN_ID') }} AS ALLOTMENT_CABIN_ID,
{{ transform_numeric('ALLOTMENT_AGREEMENT_ID') }} AS ALLOTMENT_AGREEMENT_ID,
{{ transform_string('PAX_RESERVE_TYPE') }} AS PAX_RESERVE_TYPE,
{{ transform_string('PAX_TYPE') }} AS PAX_TYPE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW2', 'SHIP_INVENTORY_ALLOC') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
