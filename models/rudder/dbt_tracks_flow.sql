/*

We leverage analytic functions like first_value and nth_value to create 20-event sequences that capture the flow of events during a session. 20 can be increased or decreased as per requirements.

*/

{{ config(materialized='table') }}

WITH derived_table AS (
    SELECT
        event_id,
        session_id,
        track_sequence_number,
        event,
        dbt_visitor_id,
        timestamp,
        LAG(event, 1) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_2,
        LAG(event, 2) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_3,
        LAG(event, 3) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_4,
        LAG(event, 4) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_5,
        LAG(event, 5) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_6,
        LAG(event, 6) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_7,
        LAG(event, 7) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_8,
        LAG(event, 8) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_9,
        LAG(event, 9) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_10,
        LAG(event, 10) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_11,
        LAG(event, 11) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_12,
        LAG(event, 12) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_13,
        LAG(event, 13) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_14,
        LAG(event, 14) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_15,
        LAG(event, 15) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_16,
        LAG(event, 16) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_17,
        LAG(event, 17) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_18,
        LAG(event, 18) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_19,
        LAG(event, 19) OVER (PARTITION BY session_id ORDER BY track_sequence_number ASC) AS event_20
    FROM {{ ref('dbt_track_facts') }}
)

SELECT
    event_id,
    session_id,
    track_sequence_number,
    event,
    dbt_visitor_id,
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    event_2,
    event_3,
    event_4,
    event_5,
    event_6,
    event_7,
    event_8,
    event_9,
    event_10,
    event_11,
    event_12,
    event_13,
    event_14,
    event_15,
    event_16,
    event_17,
    event_18,
    event_19,
    event_20
FROM derived_table a