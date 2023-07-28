{{ config(materialized='table') }}

WITH unioned_pages_tracks AS (
    {{ dbt_utils.union_relations(
        relations=get_pages_and_tracks()
    ) }}
)

SELECT *
FROM unioned_pages_tracks