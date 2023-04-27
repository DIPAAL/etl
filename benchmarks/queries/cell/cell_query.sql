WITH
    enc(geom) AS (SELECT geom, geom_geodetic FROM reference_geometries WHERE id = :AREA_ID AND type = 'enc'),
    q_window(box, start_date_id, end_date_id) AS (
        SELECT
            SetSRID(STBox(
                enc.geom,
                span(timestamp_from_date_time_id(:START_ID, 0), timestamp_from_date_time_id(:END_ID, 0), true, false)
            ), 3034) box,
            :START_ID start_date_id,
            :END_ID end_date_id
        FROM enc
    )
SELECT distinct(ds.*)
FROM q_window q
INNER JOIN fact_cell_{CELL_SIZE}m fc ON fc.st_bounding_box && q.box
INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
INNER JOIN dim_ship_type dst on ds.ship_type_id = dst.ship_type_id
WHERE fc.entry_date_id BETWEEN q.start_date_id AND q.end_date_id AND dst.ship_type = ANY(:SHIP_TYPES)
;