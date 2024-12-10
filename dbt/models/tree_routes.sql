{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key=['route', 'date'],
    event_time='date',
    begin='2024-12-01',
    batch_size='day'
) }}

-- depends_on: {{ ref('stg_encoded_tree') }}

WITH RECURSIVE tree_paths AS (
    SELECT
        *
    FROM {{ ref('stg_encoded_tree') }}
    WHERE ParentID IS NULL

    UNION ALL

    SELECT
        CONCAT(tree_paths.route, '->', nodes.NodeID::VARCHAR) AS route,
        CONCAT(tree_paths.full_path, '->', nodes.Value) AS full_path,
        nodes.NodeID,
        nodes.ParentID,
        nodes.Value,
        nodes.date
    FROM tree_paths
    JOIN (select * from {{ ref('stg_encoded_tree') }}) AS nodes
    ON tree_paths.NodeID = nodes.ParentID
    and tree_paths.date = nodes.date
)

SELECT 
    route,
    full_path,
    date
FROM tree_paths
ORDER BY route
