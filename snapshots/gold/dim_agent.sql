{% snapshot dim_agent%}

{{
    config(
        target_schema = 'SEAWARE',
        target_database = 'AMA_DEV_GLD',
        unique_key = ['AGENT_ID','DATA_SOURCE'],
        strategy = 'timestamp',
        updated_at = 'LAST_UPDATED_TIMESTAMP',
        invalidate_hard_deletes = true,
        snapshot_meta_column_names = {
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_scd_id": "dim_agent_id",
            "dbt_updated_at": "updated_at",
            "dbt_is_deleted": "is_current"
        }
    )
}}


SELECT
    DATA_SOURCE,
    AGENT_ID,
    LAST_NAME,
    FIRST_NAME,
    MIDDLE_NAME,
    FULL_NAME,
    SALUTATION,
    TITLE,
    SEX,
    BIRTHDAY,
    IATAN_NUMBER,
    COMMENTS,
    EMAIL,
    LAST_UPDATED_TIMESTAMP,
    SOURCE_DELETED,
FROM {{ ref("agent") }}


{% endsnapshot %}
