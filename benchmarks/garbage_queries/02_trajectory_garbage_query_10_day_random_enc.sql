WITH
    enc(geom) AS (SELECT geom, geom_geodetic FROM reference_geometries WHERE type = 'enc' ORDER BY random() LIMIT 1),
    q_window(box) AS (
        SELECT
            STBox(
                enc.geom_geodetic,
                (
                    SELECT period(t.start_time, t.start_time+interval '10 days') FROM (
                        SELECT '2022-01-01T00:00:00Z'::timestamptz + random() * ('2022-03-31T00:00:00Z'::timestamptz - '2022-01-01T00:00:00Z'::timestamptz) AS start_time
                    ) AS t
                )
            ) box
        FROM enc
    )
SELECT DISTINCT(ds.*)
FROM q_window
INNER JOIN dim_trajectory dt ON atStBox(dt.trajectory, SetSRID(q_window.box,4326)) IS NOT NULL
INNER JOIN fact_trajectory ft ON dt.date_id = ft.start_date_id AND dt.trajectory_sub_id = ft.trajectory_sub_id
INNER JOIN dim_ship ds ON ds.ship_id = ft.ship_id
INNER JOIN dim_ship_junk dsj ON dsj.ship_junk_id = ft.ship_junk_id
WHERE ft.start_date_id BETWEEN 20220101 AND 20220331 AND dsj.ship_type = 'Fishing'
;