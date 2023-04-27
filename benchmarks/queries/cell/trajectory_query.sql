WITH
    enc(geom, geom_geodetic) AS (SELECT geom, geom_geodetic FROM reference_geometries WHERE id = :AREA_ID AND type = 'enc'),
    q_window(box, start_date_id, end_date_id) AS (
        SELECT
            STBox(
                enc.geom_geodetic,
                span(timestamp_from_date_time_id(:START_ID, 0), timestamp_from_date_time_id(:END_ID, 0), true, false)
            ) box,
            :START_ID start_date_id,
            :END_ID end_date_id
        FROM enc
    )
SELECT DISTINCT(ds.*)
FROM q_window
INNER JOIN dim_trajectory dt ON atStBox(dt.trajectory, q_window.box) IS NOT NULL
INNER JOIN fact_trajectory ft ON dt.date_id = ft.start_date_id AND dt.trajectory_sub_id = ft.trajectory_sub_id
INNER JOIN dim_ship ds ON ds.ship_id = ft.ship_id
INNER JOIN dim_ship_type dst on ds.ship_type_id = dst.ship_type_id
WHERE ft.start_date_id BETWEEN q_window.start_date_id AND q_window.end_date_id AND dst.ship_type = ANY(:SHIP_TYPES)
;