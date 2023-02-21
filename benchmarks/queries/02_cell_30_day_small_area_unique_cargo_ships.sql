WITH
    enc(geom) AS (SELECT geom, geom_geodetic FROM reference_geometries WHERE id = 117 AND type = 'enc'),
    q_window(box, start_date_id, end_date_id) AS (
        SELECT
            SetSRID(STBox(
                enc.geom,
                period('2022-01-26 00:00:00+00', '2022-02-25 00:00:00+00')
            ),0) box,
            20220126 start_date_id,
            20220224 end_date_id
        FROM enc
    )
SELECT distinct(ds.*)
FROM q_window q
INNER JOIN fact_cell fc ON fc.st_bounding_box && q.box
INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
INNER JOIN dim_ship_junk dsj ON dsj.ship_junk_id = fc.ship_junk_id
WHERE fc.entry_date_id BETWEEN q.start_date_id AND q.end_date_id AND dsj.ship_type = 'Cargo'
;