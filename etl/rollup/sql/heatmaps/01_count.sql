-- Insert 5000m density heatmap
INSERT INTO fact_heatmap (cell_x, cell_y, date_id, time_id, ship_type_id, rast, heatmap_type_id, spatial_resolution, temporal_resolution_s, infer_stopped, division_id)
SELECT
    i2.cell_x,
    i2.cell_y,
    i2.date_id,
    (i2.entry_hour_of_day || '0000')::int AS time_id,
    i2.ship_type_id,
    i2.rast,
    (SELECT heatmap_type_id FROM dim_heatmap_type WHERE slug = 'count') AS heatmap_type_id,
    :SPATIAL_RESOLUTION AS spatial_resolution,
    :TEMPORAL_RESOLUTION AS temporal_resolution_s,
    i2.infer_stopped,
    i2.division_id
FROM
    (
        SELECT
            ST_Union(
                ST_AsRaster(
                    i1.geom,
                    ST_MakeEmptyRaster (795000, 420000, 3600000, 3055000, {CELL_SIZE}, {CELL_SIZE}, 0, 0, 3034),
                    '32BUI'::text,
                    cnt::int
                )
            ) AS rast,
            i1.cell_x / (5000 / {CELL_SIZE}) AS cell_x,
            i1.cell_y / (5000 / {CELL_SIZE}) AS cell_y,
            i1.date_id,
            i1.entry_hour_of_day,
            i1.ship_type_id,
            i1.infer_stopped,
            i1.division_id
        FROM
        (
            SELECT
                cell_x,
                cell_y,
                fc.entry_date_id AS date_id,
                dt.entry_hour_of_day,
                ds.ship_type_id,
                dc.geom AS geom,
                fc.infer_stopped,
                fc.division_id,
                COUNT(*) cnt
            FROM fact_cell_{CELL_SIZE}m fc
            INNER JOIN dim_cell_entry_time dt ON dt.entry_time_id = fc.entry_time_id
            INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
            INNER JOIN dim_cell_{CELL_SIZE}m dc ON dc.x = fc.cell_x AND dc.y = fc.cell_y AND dc.division_id = fc.division_id
            WHERE fc.entry_date_id = :DATE_KEY
            GROUP BY fc.division_id, fc.cell_x, fc.cell_y, fc.infer_stopped, fc.entry_date_id, dt.entry_hour_of_day, ds.ship_type_id, dc.geom
        ) i1
        GROUP BY i1.division_id, i1.cell_x / (5000 / {CELL_SIZE}), i1.cell_y / (5000 / {CELL_SIZE}), i1.infer_stopped, i1.date_id, i1.entry_hour_of_day, i1.ship_type_id
    ) i2
;