-- Ship in a given area and time interval: TRAJECTORY FACT
WITH q_window(box, start_date_id, end_date_id) AS (
    SELECT
    STBox(
        ST_MakeEnvelope(10.817894,57.164297, 11.287206, 57.376069, 4326),
        period('2022-01-01 00:10:00+00', '2022-01-31 23:55:00+00')
    ) box,
    20220101 start_date_id,
    20220101 end_date_id
)
SELECT DISTINCT(ds.*)
FROM q_window
INNER JOIN dim_trajectory dt ON atStBox(dt.trajectory, q_window.box) IS NOT NULL
INNER JOIN fact_trajectory ft ON dt.date_id = ft.start_date_id AND dt.trajectory_sub_id = ft.trajectory_sub_id
INNER JOIN dim_ship ds ON ds.ship_id = ft.ship_id
WHERE fc.entry_date_id >= q.start_date_id  and
	fc.entry_date_id <= q.end_date_id
;
