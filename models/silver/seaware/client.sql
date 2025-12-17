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
        unique_key=['CLIENT_ID', 'DATA_SOURCE'],
        pre_hook=[
            "{% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) %}
             {% set table_exists = target_relation is not none %}
             {% if table_exists %}
                 {% set cfg = get_config_row('SW1', this.database, this.schema, 'CLIENT') %}
                 {% set load_type_val = 'FULL' if cfg['LAST_UPDATED_WATERMARK_VALUE'] is none else 'INCREMENTAL' %}            
             {% endif %}"
        ],
        post_hook=[
            "{% if execute %}
                 {% set wm_col_sw1 = get_watermark_column('SW1', this.database, this.schema, 'CLIENT') %}
                 {% set max_wm_sw1 = compute_max_watermark_seaware(this, wm_col_sw1, 'SW1') %}
                 {% if max_wm_sw1 is not none %}
                     {% do update_config_watermark('SW1', this.database, this.schema, 'CLIENT', max_wm_sw1) %}
                 {% endif %}

                 {% set wm_col_sw2 = get_watermark_column('SW2', this.database, this.schema, 'CLIENT') %}
                 {% set max_wm_sw2 = compute_max_watermark_seaware(this, wm_col_sw2, 'SW2') %}
                 {% if max_wm_sw2 is not none %}
                     {% do update_config_watermark('SW2', this.database, this.schema, 'CLIENT', max_wm_sw2) %}
                {% endif %}
             {% endif %}"
        ]
    )
}}


{# ================================================================
   FETCH CONFIG & WATERMARK INFO
   ================================================================ #}

{% if execute %}
    {% set cfg_sw1 = get_config_row('SW1', this.database, this.schema, 'CLIENT') %}
    {% set wm_col_sw1 = cfg_sw1['WATERMARK_COLUMN'] %}
    {% set last_wm_sw1 = cfg_sw1['LAST_UPDATED_WATERMARK_VALUE'] %}
    {% set is_full_sw1 = (last_wm_sw1 is none) %}
    {% set cfg_sw2 = get_config_row('SW2', this.database, this.schema, 'CLIENT') %}
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
            {{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
            {{ transform_numeric('HOUSEHOLD_ID') }} AS HOUSEHOLD_ID,
            {{ transform_string('LAST_NAME') }} AS LAST_NAME,
            {{ transform_string('FIRST_NAME') }} AS FIRST_NAME,
            {{ transform_string('MIDDLE_NAME') }} AS MIDDLE_NAME,
            {{ transform_string('FULL_NAME') }} AS FULL_NAME,
            {{ transform_string('SALUTATION') }} AS SALUTATION,
            {{ transform_string('TITLE') }} AS TITLE,
            {{ transform_string('OCCUPATION') }} AS OCCUPATION,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_date('BIRTHDAY') }} AS BIRTHDAY,
            {{ transform_string('BIRTH_PLACE') }} AS BIRTH_PLACE,
            {{ transform_string('SEX') }} AS SEX,
            {{ transform_string('IS_HANDICAPPED') }} AS IS_HANDICAPPED,
            {{ transform_string('IS_SMOKER') }} AS IS_SMOKER,
            {{ transform_string('SOUVENIR_FNAME') }} AS SOUVENIR_FNAME,
            {{ transform_string('SOUVENIR_LNAME') }} AS SOUVENIR_LNAME,
            {{ transform_string('LANGUAGE') }} AS LANGUAGE,
            {{ transform_string('PASSPORT_NUMBER') }} AS PASSPORT_NUMBER,
            {{ transform_string('PASSPORT_ISSUE_PLACE') }} AS PASSPORT_ISSUE_PLACE,
            {{ transform_date('PASSPORT_ISSUE_DATE') }} AS PASSPORT_ISSUE_DATE,
            {{ transform_date('PASSPORT_EXP_DATE') }} AS PASSPORT_EXP_DATE,
            {{ transform_string('CITIZENSHIP') }} AS CITIZENSHIP,
            {{ transform_string('REFERRAL_TYPE') }} AS REFERRAL_TYPE,
            {{ transform_date('REFERRAL_DATE') }} AS REFERRAL_DATE,
            {{ transform_numeric('REFERRAL_CLIENT_ID') }} AS REFERRAL_CLIENT_ID,
            {{ transform_numeric('REFERRAL_HOUSEHOLD_ID') }} AS REFERRAL_HOUSEHOLD_ID,
            {{ transform_string('REFERRAL_SOURCE') }} AS REFERRAL_SOURCE,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('CLIENT_TYPE') }} AS CLIENT_TYPE,
            {{ transform_string('GUEST_TYPE') }} AS GUEST_TYPE,
            {{ transform_string('DIRECTORY_NAME') }} AS DIRECTORY_NAME,
            {{ transform_string('EMAIL') }} AS EMAIL,
            {{ transform_string('COUNTRY_OF_BIRTH') }} AS COUNTRY_OF_BIRTH,
            {{ transform_string('WEB_PASSWORD') }} AS WEB_PASSWORD,
            {{ transform_string('WEB_LOGIN_NAME') }} AS WEB_LOGIN_NAME,
            {{ transform_date('WEB_LAST_LOGIN') }} AS WEB_LAST_LOGIN,
            {{ transform_numeric('SSN') }} AS SSN,
            {{ transform_string('LANGUAGE_CODE') }} AS LANGUAGE_CODE,
            {{ transform_numeric('HISTORICAL_CRUISES_NUM') }} AS HISTORICAL_CRUISES_NUM,
            {{ transform_numeric('HISTORICAL_DAYS_NUM') }} AS HISTORICAL_DAYS_NUM,
            {{ transform_string('ALLOW_WEB_ACCESS') }} AS ALLOW_WEB_ACCESS,
            {{ transform_string('CL_ENTITY_TYPE') }} AS CL_ENTITY_TYPE,
            {{ transform_string('SEND_PROMOTIONAL_MAIL') }} AS SEND_PROMOTIONAL_MAIL,
            {{ transform_string('SEND_PROMOTIONAL_EMAIL') }} AS SEND_PROMOTIONAL_EMAIL,
            {{ transform_string('SEND_PROMOTIONAL_SMS') }} AS SEND_PROMOTIONAL_SMS,
            {{ transform_string('NOTIF_DFLT_DISTR_TYPE') }} AS NOTIF_DFLT_DISTR_TYPE,
            {{ transform_string('LAST_NAME_UPPER') }} AS LAST_NAME_UPPER,
            NULL AS HOUSEHOLD_ADDR_ID,
            NULL AS CHECK_IN_PHOTO_ID,
            NULL AS HOUSEHOLD_SEQN,
            NULL AS EMAIL_CAN_CONTACT,
            NULL AS AKA_FIRST_NAME,
            NULL AS AKA_LAST_NAME,
            {{ transform_numeric('RANK') }} AS RANK,
            NULL AS IS_DECEASED,
            NULL AS FIRST_NAME_NATIVE,
            NULL AS MIDDLE_NAME_NATIVE,
            NULL AS LAST_NAME_NATIVE,
            NULL AS RESIDENCE_COUNTRY_CODE,
            NULL AS VERIFIED_EMAIL,
            {{ transform_string('SUFFIX') }} AS SUFFIX,
            NULL AS MOBILE_PHONE_NUMBER,
            NULL AS MOBILE_INTL_CODE,
            NULL AS CLIENT_GUID,
            NULL AS WEB_LOGIN_FAILURES,
            NULL AS IS_CLIENT_TYPE_CALC_ENABLED,
            NULL AS ALT_CLIENT_ID,
            NULL AS IS_OBFUSCATED,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            {{ transform_string('NATIONALITY') }} AS NATIONALITY,
            {{ transform_string('CLIENT_CLASS_CODE') }} AS CLIENT_CLASS_CODE,
            {{ transform_string('SEND_PROMOTIONAL_PHONE') }} AS SEND_PROMOTIONAL_PHONE
    FROM {{ source('AMA_PROD_BRNZ_SW1', 'CLIENT') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw1 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw1) }}
    {% endif %}
),

sw2_src AS (
    SELECT
            'SW2' AS DATA_SOURCE,
            {{ transform_numeric('CLIENT_ID') }} AS CLIENT_ID,
            {{ transform_numeric('HOUSEHOLD_ID') }} AS HOUSEHOLD_ID,
            {{ transform_string('LAST_NAME') }} AS LAST_NAME,
            {{ transform_string('FIRST_NAME') }} AS FIRST_NAME,
            {{ transform_string('MIDDLE_NAME') }} AS MIDDLE_NAME,
            {{ transform_string('FULL_NAME') }} AS FULL_NAME,
            {{ transform_string('SALUTATION') }} AS SALUTATION,
            {{ transform_string('TITLE') }} AS TITLE,
            {{ transform_string('OCCUPATION') }} AS OCCUPATION,
            {{ transform_string('IS_ACTIVE') }} AS IS_ACTIVE,
            {{ transform_date('BIRTHDAY') }} AS BIRTHDAY,
            {{ transform_string('BIRTH_PLACE') }} AS BIRTH_PLACE,
            {{ transform_string('SEX') }} AS SEX,
            {{ transform_string('IS_HANDICAPPED') }} AS IS_HANDICAPPED,
            {{ transform_string('IS_SMOKER') }} AS IS_SMOKER,
            {{ transform_string('SOUVENIR_FNAME') }} AS SOUVENIR_FNAME,
            {{ transform_string('SOUVENIR_LNAME') }} AS SOUVENIR_LNAME,
            {{ transform_string('LANGUAGE') }} AS LANGUAGE,
            {{ transform_string('PASSPORT_NUMBER') }} AS PASSPORT_NUMBER,
            {{ transform_string('PASSPORT_ISSUE_PLACE') }} AS PASSPORT_ISSUE_PLACE,
            {{ transform_date('PASSPORT_ISSUE_DATE') }} AS PASSPORT_ISSUE_DATE,
            {{ transform_date('PASSPORT_EXP_DATE') }} AS PASSPORT_EXP_DATE,
            {{ transform_string('CITIZENSHIP') }} AS CITIZENSHIP,
            {{ transform_string('REFERRAL_TYPE') }} AS REFERRAL_TYPE,
            {{ transform_date('REFERRAL_DATE') }} AS REFERRAL_DATE,
            {{ transform_numeric('REFERRAL_CLIENT_ID') }} AS REFERRAL_CLIENT_ID,
            {{ transform_numeric('REFERRAL_HOUSEHOLD_ID') }} AS REFERRAL_HOUSEHOLD_ID,
            {{ transform_string('REFERRAL_SOURCE') }} AS REFERRAL_SOURCE,
            {{ transform_string('COMMENTS') }} AS COMMENTS,
            {{ transform_string('CLIENT_TYPE') }} AS CLIENT_TYPE,
            {{ transform_string('GUEST_TYPE') }} AS GUEST_TYPE,
            {{ transform_string('DIRECTORY_NAME') }} AS DIRECTORY_NAME,
            {{ transform_string('EMAIL') }} AS EMAIL,
            {{ transform_string('COUNTRY_OF_BIRTH') }} AS COUNTRY_OF_BIRTH,
            {{ transform_string('WEB_PASSWORD') }} AS WEB_PASSWORD,
            {{ transform_string('WEB_LOGIN_NAME') }} AS WEB_LOGIN_NAME,
            {{ transform_date('WEB_LAST_LOGIN') }} AS WEB_LAST_LOGIN,
            {{ transform_numeric('SSN') }} AS SSN,
            {{ transform_string('LANGUAGE_CODE') }} AS LANGUAGE_CODE,
            {{ transform_numeric('HISTORICAL_CRUISES_NUM') }} AS HISTORICAL_CRUISES_NUM,
            {{ transform_numeric('HISTORICAL_DAYS_NUM') }} AS HISTORICAL_DAYS_NUM,
            {{ transform_string('ALLOW_WEB_ACCESS') }} AS ALLOW_WEB_ACCESS,
            {{ transform_string('CL_ENTITY_TYPE') }} AS CL_ENTITY_TYPE,
            {{ transform_string('SEND_PROMOTIONAL_MAIL') }} AS SEND_PROMOTIONAL_MAIL,
            {{ transform_string('SEND_PROMOTIONAL_EMAIL') }} AS SEND_PROMOTIONAL_EMAIL,
            {{ transform_string('SEND_PROMOTIONAL_SMS') }} AS SEND_PROMOTIONAL_SMS,
            {{ transform_string('NOTIF_DFLT_DISTR_TYPE') }} AS NOTIF_DFLT_DISTR_TYPE,
            {{ transform_string('LAST_NAME_UPPER') }} AS LAST_NAME_UPPER,
            {{ transform_numeric('HOUSEHOLD_ADDR_ID') }} AS HOUSEHOLD_ADDR_ID,
            {{ transform_numeric('CHECK_IN_PHOTO_ID') }} AS CHECK_IN_PHOTO_ID,
            {{ transform_numeric('HOUSEHOLD_SEQN') }} AS HOUSEHOLD_SEQN,
            {{ transform_string('EMAIL_CAN_CONTACT') }} AS EMAIL_CAN_CONTACT,
            {{ transform_string('AKA_FIRST_NAME') }} AS AKA_FIRST_NAME,
            {{ transform_string('AKA_LAST_NAME') }} AS AKA_LAST_NAME,
            {{ transform_numeric('RANK') }} AS RANK,
            {{ transform_string('IS_DECEASED') }} AS IS_DECEASED,
            {{ transform_string('FIRST_NAME_NATIVE') }} AS FIRST_NAME_NATIVE,
            {{ transform_string('MIDDLE_NAME_NATIVE') }} AS MIDDLE_NAME_NATIVE,
            {{ transform_string('LAST_NAME_NATIVE') }} AS LAST_NAME_NATIVE,
            {{ transform_string('RESIDENCE_COUNTRY_CODE') }} AS RESIDENCE_COUNTRY_CODE,
            {{ transform_string('VERIFIED_EMAIL') }} AS VERIFIED_EMAIL,
            {{ transform_string('SUFFIX') }} AS SUFFIX,
            {{ transform_string('MOBILE_PHONE_NUMBER') }} AS MOBILE_PHONE_NUMBER,
            {{ transform_numeric('MOBILE_INTL_CODE') }} AS MOBILE_INTL_CODE,
            {{ transform_string('CLIENT_GUID') }} AS CLIENT_GUID,
            {{ transform_numeric('WEB_LOGIN_FAILURES') }} AS WEB_LOGIN_FAILURES,
            {{ transform_string('IS_CLIENT_TYPE_CALC_ENABLED') }} AS IS_CLIENT_TYPE_CALC_ENABLED,
            {{ transform_string('ALT_CLIENT_ID') }} AS ALT_CLIENT_ID,
            {{ transform_string('IS_OBFUSCATED') }} AS IS_OBFUSCATED,
            {{ transform_datetime('_FIVETRAN_SYNCED') }} AS LAST_UPDATED_TIMESTAMP,
            _FIVETRAN_DELETED AS SOURCE_DELETED,
            NULL AS NATIONALITY,
            NULL AS CLIENT_CLASS_CODE,
            NULL AS SEND_PROMOTIONAL_PHONE
    FROM {{ source('AMA_PROD_BRNZ_SW2', 'CLIENT') }}
    -- Incremental load: include only rows whose watermark is greater than the last recorded watermark value
    {% if is_incremental() and not is_full %}
    WHERE COALESCE({{ wm_col_sw2 }}, {{ wm_default_literal() }}) > {{ _format_watermark(last_wm_sw2) }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["CLIENT_ID", "DATA_SOURCE"]) }} AS CLIENT_SURROGATE_KEY,
    sw1_src.*
FROM sw1_src

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["CLIENT_ID", "DATA_SOURCE"]) }} AS CLIENT_SURROGATE_KEY,
    sw2_src.*
FROM sw2_src

