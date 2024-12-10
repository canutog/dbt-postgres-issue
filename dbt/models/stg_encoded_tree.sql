{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key=['NodeID', 'ParentID', 'date'],
    event_time='date',
    begin='2024-12-01',
    batch_size='day'
) }}
WITH final AS (
    SELECT
        "NodeID"::VARCHAR AS route,
        "Value"::VARCHAR AS full_path,
        "NodeID" as NodeID,
        "ParentID" as ParentID,
        "Value" as Value,
        "date" as date
    FROM {{ ref('encoded_tree_example') }}
    WHERE "ParentID" IS NULL

)

select * from final

