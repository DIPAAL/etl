WITH
    enc(geom) AS (SELECT geom, geom_geodetic FROM enc_cells ORDER BY random() LIMIT 1),
    q_window(box) AS (
        SELECT
            SetSRID(STBox(
                enc.geom,
                (
                    SELECT period(t.start_time, t.start_time+interval '10 days') FROM (
                        SELECT '2022-01-01T00:00:00Z'::timestamptz + random() * ('2022-03-31T00:00:00Z'::timestamptz - '2022-01-01T00:00:00Z'::timestamptz) AS start_time
                    ) AS t
                )
            ),0) box
        FROM enc
    )
SELECT distinct(ds.*)
FROM q_window q
INNER JOIN fact_cell fc ON fc.st_bounding_box && q.box
INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
INNER JOIN dim_ship_junk dsj ON dsj.ship_junk_id = fc.ship_junk_id
WHERE fc.entry_date_id BETWEEN 20220101 AND 20220331 AND dsj.ship_type = 'Fishing'
;