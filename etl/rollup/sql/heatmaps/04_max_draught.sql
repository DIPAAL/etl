-- Insert 5000m density heatmap
INSERT INTO fact_cell_heatmap (cell_x, cell_y, date_id, time_id, ship_type_id, rast, heatmap_type_id, spatial_resolution, temporal_resolution_sec, infer_stopped, partition_id)
SELECT
    i2.cell_x,
    i2.cell_y,
    i2.date_id,
    (i2.hour_of_day || '0000')::int AS time_id,
    i2.ship_type_id,
    i2.rast,
    (SELECT heatmap_type_id FROM dim_heatmap_type WHERE slug = 'max_draught') AS heatmap_type_id,
    %(SPATIAL_RESOLUTION)s AS spatial_resolution,
    %(TEMPORAL_RESOLUTION)s AS temporal_resolution_sec,
    i2.infer_stopped,
    i2.partition_id
FROM
    (
        SELECT
            ST_Union(
                ST_AsRaster(
                    i1.geom,
                    ST_MakeEmptyRaster (795000, 420000, 3600000, 3055000, {CELL_SIZE}, {CELL_SIZE}, 0, 0, 3034),
                    '32BF'::text,
                    max_draught
                )
            ) AS rast,
            i1.cell_x / (5000 / {CELL_SIZE}) AS cell_x,
            i1.cell_y / (5000 / {CELL_SIZE}) AS cell_y,
            i1.date_id,
            i1.hour_of_day,
            i1.ship_type_id,
            i1.infer_stopped,
            i1.partition_id
        FROM
        (
            SELECT
                cell_x,
                cell_y,
                fc.entry_date_id AS date_id,
                dt.hour_of_day,
                ds.ship_type_id,
                dc.geom AS geom,
                fc.infer_stopped,
                fc.partition_id,
                max(fc.draught) AS max_draught
            FROM fact_cell_{CELL_SIZE}m fc
            INNER JOIN dim_time dt ON dt.time_id = fc.entry_time_id
            INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
            INNER JOIN dim_cell_{CELL_SIZE}m dc ON dc.x = fc.cell_x AND dc.y = fc.cell_y AND dc.partition_id = fc.partition_id
            WHERE fc.entry_date_id = %(DATE_KEY)s
            GROUP BY fc.partition_id, fc.cell_x, fc.cell_y, fc.infer_stopped, fc.entry_date_id, dt.hour_of_day, ds.ship_type_id, dc.geom
        ) i1
        GROUP BY i1.partition_id, i1.cell_x / (5000 / {CELL_SIZE}), i1.cell_y / (5000 / {CELL_SIZE}), i1.infer_stopped, i1.date_id, i1.hour_of_day, i1.ship_type_id
    ) i2
;