-- Ship in a given area and time interval: CELL FACT
-- Measured to ~0.3s
WITH q_window(box, start_date_id, end_date_id) AS (
    SELECT
    STBox(
        ST_Transform(ST_MakeEnvelope(10.817894,57.164297, 11.287206, 57.376069, 4326),3034),
        period('2022-01-01 00:10:00+00', '2022-01-31 23:55:00+00')
    ) box,
    20220101 start_date_id,
    20220101 end_date_id
)
SELECT distinct(ds.*)
FROM q_window q
INNER JOIN fact_cell fc ON fc.st_bounding_box && q.box
INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
WHERE fc.entry_date_id >= q.start_date_id  and
	fc.entry_date_id <= q.end_date_id
;