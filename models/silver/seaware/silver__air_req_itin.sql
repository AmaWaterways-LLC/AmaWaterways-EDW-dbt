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
        unique_key=['_FIVETRAN_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'AIR_REQ_ITIN') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_air_req_itin',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='AIR_REQ_ITIN',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'AIR_REQ_ITIN') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'AIR_REQ_ITIN', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'AIR_REQ_ITIN') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'AIR_REQ_ITIN', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'AIR_REQ_ITIN') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'AIR_REQ_ITIN') %}
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

with src as (
    select
    'SW1' AS DATA_SOURCE,
    {{ transform_numeric('PSGR_ID') }} AS PSGR_ID,
    {{ transform_numeric('RTG_REQ_SEQ') }} AS RTG_REQ_SEQ,
    {{ transform_string('FLGT_NR') }} AS FLGT_NR,
    {{ transform_string('DEP_CTY_CD') }} AS DEP_CTY_CD,
    {{ transform_string('ARR_CTY_CD') }} AS ARR_CTY_CD,
    {{ transform_datetime('DEP_DT') }} AS DEP_DT,
    {{ transform_datetime('ARR_DT') }} AS ARR_DT,
    {{ transform_datetime('DEP_TM') }} AS DEP_TM,
    {{ transform_datetime('ARR_TM') }} AS ARR_TM,
    {{ transform_string('INT_STS') }} AS INT_STS,
    {{ transform_string('INV_CLOS') }} AS INV_CLOS,
    {{ transform_string('BKG_CRS_CD') }} AS BKG_CRS_CD,
    {{ transform_string('CNF_CRS_CD') }} AS CNF_CRS_CD,
    {{ transform_string('BKG_RLOC') }} AS BKG_RLOC,
    {{ transform_string('CNF_RLOC') }} AS CNF_RLOC,
    {{ transform_datetime('ADV_DT') }} AS ADV_DT,
    {{ transform_string('MEAL_SRV_CD') }} AS MEAL_SRV_CD,
    {{ transform_string('EQUIP_CD') }} AS EQUIP_CD,
    {{ transform_numeric('NR_STOPS') }} AS NR_STOPS,
    {{ transform_string('SCHED_SRV') }} AS SCHED_SRV,
    {{ transform_string('CHARTER_FLG') }} AS CHARTER_FLG,
    {{ transform_string('CARR_CD') }} AS CARR_CD,
    {{ transform_numeric('ITIN_IDX') }} AS ITIN_IDX,
    {{ transform_numeric('RECORD_ID') }} AS RECORD_ID,
    {{ transform_string('DIRECTION') }} AS DIRECTION,
    {{ transform_string('IS_SHELL_PNR') }} AS IS_SHELL_PNR,
    {{ transform_string('IS_NEGO_BLOCK') }} AS IS_NEGO_BLOCK,
    {{ transform_string('IS_CHARTER') }} AS IS_CHARTER,
    {{ transform_string('CONF_STATUS') }} AS CONF_STATUS,
    {{ transform_string('SEAT_NUMBER') }} AS SEAT_NUMBER,
    {{ transform_string('TICKET_NUMBER') }} AS TICKET_NUMBER,
    {{ transform_string('CLOS_INFO') }} AS CLOS_INFO,
     _FIVETRAN_DELETED AS SOURCE_DELETED,
    {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP
    from {{ source('AMA_PROD_BRNZ_SW1', 'AIR_REQ_ITIN') }}
        {% if is_incremental() and not is_full %}
        where coalesce({{ wm_col }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm) }}
        {% endif %}
)
select * 
from src
