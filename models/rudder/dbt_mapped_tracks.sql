/*

 Use the ID generated while creating dbt_aliases_mapping to link all events for the same user on that device. Also note the idle time between events

*/

{{ config(materialized='table') }}

select *
        ,DATEDIFF('minute', LAG(timestamp) OVER (PARTITION BY dbt_visitor_id ORDER BY timestamp), timestamp) AS idle_time_minutes
      from (
        select t.id as event_id
          ,t.anonymous_id
          ,a2v.dbt_visitor_id
          ,t.timestamp
          ,t.event as event
        from {{ ref('unioned_pages_tracks') }}  as t
        inner join {{ ref('dbt_aliases_mapping') }} as a2v
        on a2v.alias = coalesce(t.user_id, t.anonymous_id)
        )