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
        unique_key=['GUEST_ID', '_FIVETRAN_ID'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', target.database, target.schema, 'RES_GUEST') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}
                 {% do audit_start(
                     pipeline_name='bronze_to_silver_res_guest',
                     source_name='AUTO',
                     database_name=target.database,
                     schema_name=target.schema,
                     table_name='RES_GUEST',
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
                 {% set wm_col_sw1 = get_watermark_column('SW1', target.database, target.schema, 'RES_GUEST') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', target.database, target.schema, 'RES_GUEST', max_wm) %}
                 {% endif %}

                {% set wm_col_sw2 = get_watermark_column('SW2', target.database, target.schema, 'RES_GUEST') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', target.database, target.schema, 'RES_GUEST', max_wm) %}
                 {% endif %}
             {% endif %}"
        ]
    )
}}

{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', target.database, target.schema, 'RES_GUEST') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm is none) %}
    {% set cfg_sw2 = get_config_row('SW2', target.database, target.schema, 'RES_GUEST') %}
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
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_numeric('GUEST_SEQN') }} AS GUEST_SEQN,
{{ transform_numeric('GUEST_SEQN') }} AS GUEST_SEQN,
{{ transform_numeric('OCCUPANCY_NUMBER') }} AS OCCUPANCY_NUMBER,
{{ transform_numeric('OCCUPANCY_NUMBER') }} AS OCCUPANCY_NUMBER,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
{{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
{{ transform_string('GUEST_TYPE') }} AS GUEST_TYPE,
{{ transform_string('GUEST_TYPE') }} AS GUEST_TYPE,
{{ transform_numeric('AGE') }} AS AGE,
{{ transform_numeric('AGE') }} AS AGE,
NULL AS AGE_CATEGORY,
NULL AS AGE_CATEGORY,
NULL AS GENDER,
NULL AS GENDER,
NULL AS IS_DUMMY,
NULL AS IS_DUMMY,
NULL AS PARENT_GUEST_ID,
NULL AS PARENT_GUEST_ID,
NULL AS SECRET_SEED,
NULL AS SECRET_SEED,
NULL AS SECRET_CODE,
NULL AS SECRET_CODE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
{{ transform_string('FULL_NAME') }} AS FULL_NAME,
{{ transform_string('FULL_NAME') }} AS FULL_NAME
    from {{ source('AMA_PROD_BRNZ_SW1', 'RES_GUEST') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src as (
    select
    'SW2' AS DATA_SOURCE,
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_numeric('GUEST_ID') }} AS GUEST_ID,
{{ transform_numeric('GUEST_SEQN') }} AS GUEST_SEQN,
{{ transform_numeric('GUEST_SEQN') }} AS GUEST_SEQN,
{{ transform_numeric('OCCUPANCY_NUMBER') }} AS OCCUPANCY_NUMBER,
{{ transform_numeric('OCCUPANCY_NUMBER') }} AS OCCUPANCY_NUMBER,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('RES_ID') }} AS RES_ID,
{{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
{{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
{{ transform_string('GUEST_TYPE') }} AS GUEST_TYPE,
{{ transform_string('GUEST_TYPE') }} AS GUEST_TYPE,
{{ transform_numeric('AGE') }} AS AGE,
{{ transform_numeric('AGE') }} AS AGE,
{{ transform_string('AGE_CATEGORY') }} AS AGE_CATEGORY,
{{ transform_string('AGE_CATEGORY') }} AS AGE_CATEGORY,
{{ transform_string('GENDER') }} AS GENDER,
{{ transform_string('GENDER') }} AS GENDER,
{{ transform_string('IS_DUMMY') }} AS IS_DUMMY,
{{ transform_string('IS_DUMMY') }} AS IS_DUMMY,
{{ transform_numeric('PARENT_GUEST_ID') }} AS PARENT_GUEST_ID,
{{ transform_numeric('PARENT_GUEST_ID') }} AS PARENT_GUEST_ID,
{{ transform_numeric('SECRET_SEED') }} AS SECRET_SEED,
{{ transform_numeric('SECRET_SEED') }} AS SECRET_SEED,
{{ transform_string('SECRET_CODE') }} AS SECRET_CODE,
{{ transform_string('SECRET_CODE') }} AS SECRET_CODE,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
{{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
 _FIVETRAN_DELETED AS SOURCE_DELETED,
NULL AS FULL_NAME,
NULL AS FULL_NAME
    from {{ source('AMA_PROD_BRNZ_SW2', 'RES_GUEST') }}
    {% if is_incremental() and not is_full %}
    where coalesce({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)
    select * from sw1_src
    union all
    select * from sw2_src
