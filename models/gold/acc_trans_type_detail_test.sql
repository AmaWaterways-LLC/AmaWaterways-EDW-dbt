{{
    config(
    materialized='table'
)
}}

with cte as (
    select
            actd.DATA_SOURCE, 
            actd.TRANS_ID, 
            actd.TRANS_STATUS, 
            actd.FORM_OF_TRANS, 
            actd.TRANS_TYPE, 
            actd.SOURCE_ENTITY_TYPE, 
            actd.SOURCE_ENTITY_ID, 
            actd.DEST_ENTITY_TYPE, 
            actd.DEST_ENTITY_ID, 
            actd.EXTERNAL_IDENT, 
            actd.BATCH_HDR_ID, 
            actd.TRANS_TIME_STAMP, 
            actd.TRANS_GROUP_ID, 
            actd.COMMENTS,
            actt.TRANS_TYPE_ID,  
            actt.IS_GSA_COMMISSION, 
            actt.IS_STD_COMMISSION, 
            actt.TRANS_TYPE_CLASS, 
            actt.COMMISS_TYPE, 
            actt.REFERENCE_CODE

    from {{ ref('acc_trans_detail') }} actd
    left join {{ ref('acc_trans_type') }} actt
    on actd.TRANS_TYPE = actt.TRANS_TYPE
    and actd.DATA_SOURCE = actt.DATA_SOURCE

)
select *
from cte