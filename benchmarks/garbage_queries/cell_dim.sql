WITH q_window(box) AS (
    SELECT
        SetSRID(STBox(
            geometry :geom,
            (SELECT span(timestamptz :start_time, timestamptz :end_time))
            ),0) box
    )
SELECT
    dc.*,
    dd.*,
    dns.*
FROM q_window q
INNER JOIN fact_cell_{CELL_SIZE}m fc ON fc.st_bounding_box && setsrid(q.box, 3034)
INNER JOIN dim_cell_{CELL_SIZE}m dc ON dc.x = fc.cell_x AND dc.y = fc.cell_y AND dc.partition_id = fc.partition_id
INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
INNER JOIN dim_ship_type dst ON dst.ship_type_id = ds.ship_type_id
INNER JOIN dim_direction dd ON dd.direction_id = fc.direction_id
INNER JOIN dim_nav_status dns ON dns.nav_status_id = fc.nav_status_id
WHERE fc.entry_date_id BETWEEN :start_date_id AND :end_date_id
  AND dst.ship_type = ANY(:ship_types)
;