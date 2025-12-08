{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="_FIVETRAN_ID",
        database="PC_DBT_DB",
        schema="DBT_MKOMPELLI"
    )
}}

with source as (
    select
            trim(_fivetran_id) as _fivetran_id,
            trim(pap_code) as pap_code,
            pap_id,
            priority,
            trim(program_currency) as program_currency,
            trim(agency_domestic) as agency_domestic,
            trim(pap_type) as pap_type,
            trim(comments) as comments,
            trim(pap_name) as pap_name,
            trim(external_rules) as external_rules,
            trim(is_price_program) as is_price_program,
            trim(promo_group) as promo_group,
            trim(coupon_class) as coupon_class,
            trim(print_cruise_fare_amount) as print_cruise_fare_amount,
            trim(manual_input_value_type) as manual_input_value_type,
            trim(is_active) as is_active,
            trim(best_fare_eligible) as best_fare_eligible,
            to_date(_fivetran_synced) as _fivetran_synced,
            _fivetran_deleted
    from {{ source('db_source','pap_master')}}
{% if is_incremental() %}
    where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
{% endif %}
)

select * from source
