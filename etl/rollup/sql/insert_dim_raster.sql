INSERT INTO dim_raster (raster_id, partition_id, rast)
SELECT
    uuid_generate_v5(
            uuid_ns_url(),
            (
                    i2.cell_x::text ||
                    i2.cell_y::text ||
                    %(DATE_KEY)s ||
                    i2.hour_of_day::text || '0000' ||
                    '{CELL_SIZE}' ||
                    '86400' ||
                    i2.ship_type_id::text ||
                    (SELECT heatmap_type_id FROM dim_heatmap_type WHERE name = 'count')::text
                )::text
        ) AS raster_id,
    i2.partition_id,
    i2.rast
FROM
    (
        SELECT
            ST_Union(
                ST_AsRaster(
                    i1.geom,
                    ST_MakeEmptyRaster (795000, 420000, 3600000, 3055000, 1000, 1000, 0, 0, 3034),
                    '32BUI'::text,
                    cnt::int
                )
            ) AS rast,
            i1.cell_x / (5000 / {CELL_SIZE}) AS cell_x,
            i1.cell_y / (5000 / {CELL_SIZE}) AS cell_y,
            i1.partition_id,
            i1.hour_of_day,
            i1.ship_type_id
        FROM
        (
            SELECT
                cell_x,
                cell_y,
                fc.partition_id,
                dt.hour_of_day,
                ds.ship_type_id,
                ST_SetSRID(dc.geom,3034) AS geom,
                COUNT(*) cnt
            FROM fact_cell_{CELL_SIZE}m fc
            INNER JOIN dim_time dt ON dt.time_id = fc.entry_time_id
            INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
            INNER JOIN dim_cell_{CELL_SIZE}m dc ON dc.x = fc.cell_x AND dc.y = fc.cell_y AND dc.partition_id = fc.partition_id
            WHERE fc.entry_date_id = %(DATE_KEY)s
            GROUP BY fc.cell_x, fc.cell_y, fc.partition_id, dt.hour_of_day, ds.ship_type_id, dc.geom
        ) i1
        GROUP BY i1.cell_x / (5000 / {CELL_SIZE}), i1.cell_y / (5000 / {CELL_SIZE}), partition_id, i1.hour_of_day, i1.ship_type_id
    ) i2
;


