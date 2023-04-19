WITH
    enc(geom) AS (SELECT geom, geom_geodetic FROM reference_geometries WHERE type = 'enc' ORDER BY random() LIMIT 1),
    q_window(box) AS (
        SELECT
            SetSRID(STBox(
                enc.geom,
                (
                    SELECT span(t.start_time, t.start_time+interval '10 days') FROM (
                        SELECT '2022-01-01T00:00:00Z'::timestamptz + random() * ('2022-03-31T00:00:00Z'::timestamptz - '2022-01-01T00:00:00Z'::timestamptz) AS start_time
                    ) AS t
                )
            ),0) box
        FROM enc
    )
SELECT distinct(ds.*)
FROM q_window q
INNER JOIN fact_cell_5000m fc ON fc.st_bounding_box && q.box
INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
INNER JOIN dim_ship_type dst ON dst.ship_type_id = ds.ship_type_id
WHERE fc.entry_date_id BETWEEN 20220101 AND 20220331
  AND dst.ship_type IN (SELECT ship_type FROM dim_ship_type ORDER BY random() LIMIT floor(random() * 8 + 2)::integer) -- random between 2 and 10
;