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
        unique_key=['TREE_NODE_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'RES_HIST_TREE') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_res_hist_tree',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='RES_HIST_TREE',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'RES_HIST_TREE') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'RES_HIST_TREE', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'RES_HIST_TREE') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'RES_HIST_TREE', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'RES_HIST_TREE') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'RES_HIST_TREE') %}
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
    {{ transform_numeric('TREE_NODE_ID') }} AS TREE_NODE_ID,
    {{ transform_numeric('NODE_LEVEL') }} AS NODE_LEVEL,
    {{ transform_string('NODE_NAME') }} AS NODE_NAME,
    {{ transform_numeric('TRANS_ID_INSERT') }} AS TRANS_ID_INSERT,
    {{ transform_numeric('TRANS_ID_DELETE') }} AS TRANS_ID_DELETE,
    {{ transform_string('KEY_LEVEL_1') }} AS KEY_LEVEL_1,
    {{ transform_string('KEY_LEVEL_2') }} AS KEY_LEVEL_2,
    {{ transform_string('KEY_LEVEL_3') }} AS KEY_LEVEL_3,
    {{ transform_string('KEY_LEVEL_4') }} AS KEY_LEVEL_4,
    {{ transform_string('KEY_LEVEL_5') }} AS KEY_LEVEL_5,
    {{ transform_string('KEY_LEVEL_6') }} AS KEY_LEVEL_6,
     _FIVETRAN_DELETED AS SOURCE_DELETED,
    {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP
    from {{ source('AMA_PROD_BRNZ_SW1', 'RES_HIST_TREE') }}
        {% if is_incremental() and not is_full %}
        where coalesce({{ wm_col }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm) }}
        {% endif %}
)
select * 
from src
