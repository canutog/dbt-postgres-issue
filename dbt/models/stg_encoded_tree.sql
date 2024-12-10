{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key=['NodeID', 'ParentID', 'date'],
    event_time='date',
    begin='2024-12-01',
    batch_size='day'
) }}
WITH final AS (
    SELECT distinct
        "NodeID" as NodeID,
        "ParentID" as ParentID,
        "Value" as Value,
        "date" as date
    FROM {{ ref('encoded_tree_example') }}
)

select * from final

