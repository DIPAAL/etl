WITH
    q_window(box) AS (
        SELECT
            STBox(
                ST_MakeEnvelope(:XMIN,:YMIN, :XMAX, :YMAX, 3034),
                span(timestamp_from_date_time_id(:START_ID, 0), timestamp_from_date_time_id(:END_ID, 0), true, false)
            ) box
    )
SELECT distinct(ds.*)
FROM q_window q
INNER JOIN fact_cell_{CELL_SIZE}m fc ON fc.st_bounding_box && q.box
INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
INNER JOIN dim_ship_type dst on ds.ship_type_id = dst.ship_type_id
WHERE fc.entry_date_id BETWEEN :START_ID AND :END_ID AND dst.ship_type = ANY(:SHIP_TYPES)
;