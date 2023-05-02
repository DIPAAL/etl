WITH q_window(box) AS (
    SELECT
        STBox(
            geometry :geom_geodetic,
            (SELECT span(timestamptz :start_time, timestamptz :start_time + interval '10 days'))
        ) box
    )
SELECT DISTINCT(ds.*)
FROM q_window
INNER JOIN dim_trajectory dt ON atStBox(dt.trajectory, SetSRID(q_window.box,4326)) IS NOT NULL
INNER JOIN fact_trajectory ft ON dt.date_id = ft.start_date_id AND dt.trajectory_sub_id = ft.trajectory_sub_id
INNER JOIN dim_ship ds ON ds.ship_id = ft.ship_id
INNER JOIN dim_ship_type dst ON dst.ship_type_id = ds.ship_type_id
WHERE ft.start_date_id BETWEEN 20210101 AND 20211231 
  AND dst.ship_type = ANY (:ship_types)
;