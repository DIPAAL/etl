WITH
    enc(geom, geom_geodetic) AS (SELECT geom, geom_geodetic FROM reference_geometries WHERE id = 148 AND type = 'enc'),
    q_window(box, start_date_id, end_date_id) AS (
        SELECT
            STBox(
                enc.geom_geodetic,
                period('2022-01-10 00:00:00+00', '2022-01-11 00:00:00+00')
            ) box,
            20220110 start_date_id,
            20220110 end_date_id
        FROM enc
    )
SELECT DISTINCT(ds.*)
FROM q_window
INNER JOIN dim_trajectory dt ON atStBox(dt.trajectory, q_window.box) IS NOT NULL
INNER JOIN fact_trajectory ft ON dt.date_id = ft.start_date_id AND dt.trajectory_sub_id = ft.trajectory_sub_id
INNER JOIN dim_ship ds ON ds.ship_id = ft.ship_id
INNER JOIN dim_ship_junk dsj ON dsj.ship_junk_id = ft.ship_junk_id
WHERE ft.start_date_id BETWEEN q_window.start_date_id AND q_window.end_date_id AND dsj.ship_type = 'Cargo'
;
