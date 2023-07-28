/*

Table containing a useful session metric - session duration

*/

{{ config(materialized='view') }}

select 
    s1.dbt_visitor_id
    , s1.session_id
    , DATEDIFF('minute', CAST(s1.session_start_at AS TIMESTAMP), CAST(s2.ended_at AS TIMESTAMP)) AS session_duration
from
    {{ ref('dbt_session_tracks')}} as s1
    LEFT JOIN {{ ref('dbt_session_track_facts') }} as s2
      ON s1.session_id = s2.session_id
      
    