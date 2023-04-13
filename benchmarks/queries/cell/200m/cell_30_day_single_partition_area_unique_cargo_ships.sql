WITH
    enc(geom) AS (SELECT geom, geom_geodetic FROM reference_geometries WHERE id = 152 AND type = 'enc'),
    q_window(box, start_date_id, end_date_id) AS (
        SELECT
            SetSRID(STBox(
                enc.geom,
                span(timestamp_from_date_time_id(20220126, 0), timestamp_from_date_time_id(20220224, 0), true, false)
            ), 3034) box,
            20220126 start_date_id,
            20220224 end_date_id
        FROM enc
    )
SELECT distinct(ds.*)
FROM q_window q
INNER JOIN fact_cell_200m fc ON fc.st_bounding_box && q.box
INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
INNER JOIN dim_ship_type dst on ds.ship_type_id = dst.ship_type_id
WHERE fc.entry_date_id BETWEEN q.start_date_id AND q.end_date_id AND dst.ship_type = 'Cargo'
;