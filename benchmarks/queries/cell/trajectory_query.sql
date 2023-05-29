WITH
    q_window(box) AS (
        SELECT
            STBox(
                ST_Transform(ST_MakeEnvelope(:XMIN,:YMIN, :XMAX, :YMAX, 3034), 4326),
                span(timestamp_from_date_time_id(:START_ID, 0), timestamp_from_date_time_id(:END_ID, 0), true, false)
            ) box
    )
SELECT DISTINCT(ds.*)
FROM q_window
INNER JOIN dim_trajectory dt ON setSrid(q_window.box, 4326) && dt.trajectory AND atStBox(dt.trajectory, setSrid(q_window.box,4326)) IS NOT NULL
INNER JOIN fact_trajectory ft ON dt.date_id = ft.start_date_id AND dt.trajectory_sub_id = ft.trajectory_sub_id
INNER JOIN dim_ship ds ON ds.ship_id = ft.ship_id
INNER JOIN dim_ship_type dst on ds.ship_type_id = dst.ship_type_id
WHERE ft.start_date_id BETWEEN :START_ID AND :END_ID
  AND dt.date_id BETWEEN 20210101 AND 20211231
  AND dst.ship_type = ANY(:SHIP_TYPES)
;