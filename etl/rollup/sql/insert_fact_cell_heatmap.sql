INSERT INTO fact_cell_heatmap (cell_x, cell_y, partition_id, date_id, time_id, ship_type_id, raster_id, heatmap_type_id, spatial_resolution, temporal_resolution_sec)
SELECT
    i2.cell_x,
    i2.cell_y,
    i2.partition_id,
    %(DATE_KEY)s AS date_id,
    (i2.hour_of_day || '0000')::int AS time_id,
    i2.ship_type_id,
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
    (SELECT heatmap_type_id FROM dim_heatmap_type WHERE name = 'count') AS heatmap_type_id,
    {CELL_SIZE} AS spatial_resolution,
    86400 AS temporal_resolution_sec
FROM
    (
        SELECT
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
                ds.ship_type_id
            FROM fact_cell_{CELL_SIZE}m fc
            INNER JOIN dim_time dt ON dt.time_id = fc.entry_time_id
            INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
            INNER JOIN dim_cell_{CELL_SIZE}m dc ON dc.x = fc.cell_x AND dc.y = fc.cell_y AND dc.partition_id = fc.partition_id
            WHERE fc.entry_date_id = %(DATE_KEY)s
            GROUP BY fc.cell_x, fc.cell_y, fc.partition_id, dt.hour_of_day, ds.ship_type_id
        ) i1
        GROUP BY i1.cell_x / (5000 / {CELL_SIZE}), i1.cell_y / (5000 / {CELL_SIZE}), partition_id, i1.hour_of_day, i1.ship_type_id
    ) i2
;