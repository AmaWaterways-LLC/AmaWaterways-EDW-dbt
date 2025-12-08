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
        unique_key=['_FIVETRAN_ID', 'SHIP_REQUEST_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'RES_SHIP_REQUEST') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_res_ship_request',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='RES_SHIP_REQUEST',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'RES_SHIP_REQUEST') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'RES_SHIP_REQUEST', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'RES_SHIP_REQUEST') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'RES_SHIP_REQUEST', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'RES_SHIP_REQUEST') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'RES_SHIP_REQUEST') %}
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
{{ transform_numeric('SHIP_REQUEST_ID') }} AS SHIP_REQUEST_ID,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
{{ transform_numeric('REL_DAY_FROM') }} AS REL_DAY_FROM,
{{ transform_numeric('REL_DAY_TO') }} AS REL_DAY_TO,
{{ transform_datetime('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
{{ transform_datetime('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
{{ transform_string('CABIN_CATEGORY') }} AS CABIN_CATEGORY,
{{ transform_numeric('CABIN_SEQ_NUMBER') }} AS CABIN_SEQ_NUMBER,
{{ transform_string('INVENTORY_REQUEST_TYPE') }} AS INVENTORY_REQUEST_TYPE,
{{ transform_numeric('ALLOCATION_ID') }} AS ALLOCATION_ID,
{{ transform_string('INVENTORY_RESULT_TYPE') }} AS INVENTORY_RESULT_TYPE,
NULL AS DEP_REF_ID,
NULL AS ARR_REF_ID,
NULL AS PRICE_CATEGORY,
NULL AS CHILD_BED_REQUESTED,
NULL AS MAX_CABIN_OCCUPANCY,
NULL AS RESERVE_TYPE,
NULL AS GENDER,
NULL AS BERTHS,
NULL AS IS_NO_SHOW,
NULL AS ALLOTMENT_AGREEMENT_ID,
NULL AS FZ_COUNT,
NULL AS PAX_RESERVE_TYPE,
NULL AS REQUESTED_CABIN,
NULL AS BERTHS_RESERVED,
NULL AS BLOCK_ASSIGNMENT,
NULL AS PAX_TYPE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW1', 'RES_SHIP_REQUEST') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('SHIP_REQUEST_ID') }} AS SHIP_REQUEST_ID,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_string('SHIP_CODE') }} AS SHIP_CODE,
{{ transform_numeric('REL_DAY_FROM') }} AS REL_DAY_FROM,
{{ transform_numeric('REL_DAY_TO') }} AS REL_DAY_TO,
{{ transform_datetime('SAIL_DATE_FROM') }} AS SAIL_DATE_FROM,
{{ transform_datetime('SAIL_DATE_TO') }} AS SAIL_DATE_TO,
{{ transform_string('CABIN_CATEGORY') }} AS CABIN_CATEGORY,
{{ transform_numeric('CABIN_SEQ_NUMBER') }} AS CABIN_SEQ_NUMBER,
{{ transform_string('INVENTORY_REQUEST_TYPE') }} AS INVENTORY_REQUEST_TYPE,
{{ transform_numeric('ALLOCATION_ID') }} AS ALLOCATION_ID,
{{ transform_string('INVENTORY_RESULT_TYPE') }} AS INVENTORY_RESULT_TYPE,
{{ transform_numeric('DEP_REF_ID') }} AS DEP_REF_ID,
{{ transform_numeric('ARR_REF_ID') }} AS ARR_REF_ID,
{{ transform_string('PRICE_CATEGORY') }} AS PRICE_CATEGORY,
{{ transform_string('CHILD_BED_REQUESTED') }} AS CHILD_BED_REQUESTED,
{{ transform_numeric('MAX_CABIN_OCCUPANCY') }} AS MAX_CABIN_OCCUPANCY,
{{ transform_string('RESERVE_TYPE') }} AS RESERVE_TYPE,
{{ transform_string('GENDER') }} AS GENDER,
{{ transform_numeric('BERTHS') }} AS BERTHS,
{{ transform_string('IS_NO_SHOW') }} AS IS_NO_SHOW,
{{ transform_numeric('ALLOTMENT_AGREEMENT_ID') }} AS ALLOTMENT_AGREEMENT_ID,
{{ transform_numeric('FZ_COUNT') }} AS FZ_COUNT,
{{ transform_string('PAX_RESERVE_TYPE') }} AS PAX_RESERVE_TYPE,
{{ transform_string('REQUESTED_CABIN') }} AS REQUESTED_CABIN,
{{ transform_numeric('BERTHS_RESERVED') }} AS BERTHS_RESERVED,
{{ transform_string('BLOCK_ASSIGNMENT') }} AS BLOCK_ASSIGNMENT,
{{ transform_string('PAX_TYPE') }} AS PAX_TYPE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED
    from {{ source('AMA_PROD_BRNZ_SW2', 'RES_SHIP_REQUEST') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
