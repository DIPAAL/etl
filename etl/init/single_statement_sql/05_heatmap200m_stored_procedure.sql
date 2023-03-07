CREATE OR REPLACE PROCEDURE create_heatmap_200m (
    t_res INTEGER,
    s_res INTEGER,
    date_key INTEGER,
    part_id SMALLINT
    )
LANGUAGE plpgsql
AS $$
DECLARE
    rast_id INTEGER;
    heatmap RECORD;
    ref_rast RASTER;
BEGIN
    ref_rast := ST_MakeEmptyRaster (795000, 420000, 3600000, 3055000, 1000, 1000, 0, 0, 3034);

    -- Create table with heatmap data
    CREATE TEMPORARY TABLE heatmap_data AS
        SELECT
            ST_Union(
                ST_AsRaster(
                    i1.geom,
                    ref_rast,
                    '32BUI'::text,
                    cnt::int
                )
            ) AS rast,
            i1.cell_x / (5000 / 200) AS cell_x,
            i1.cell_y / (5000 / 200) AS cell_y,
            (i1.hour_of_day || '0000')::int AS time_id,
            i1.ship_type_id,
            (SELECT heatmap_type_id FROM dim_heatmap_type WHERE name = 'count') AS heatmap_type_id
        FROM
        (
            SELECT
                cell_x,
                cell_y,
                dt.hour_of_day,
                ds.ship_type_id,
                ST_SETSRID(dc.geom, 3034) AS geom,
                COUNT(*) cnt
            FROM fact_cell_200m fc
            INNER JOIN dim_time dt ON dt.time_id = fc.entry_time_id
            INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
            INNER JOIN dim_cell_200m dc ON dc.x = fc.cell_x AND dc.y = fc.cell_y AND dc.partition_id = fc.partition_id
            WHERE fc.entry_date_id = date_key
              AND fc.partition_id = part_id
            GROUP BY fc.cell_x, fc.cell_y, dt.hour_of_day, ds.ship_type_id, dc.geom
        ) i1
        GROUP BY i1.cell_x / (5000 / 200), i1.cell_y / (5000 / 200), i1.hour_of_day, i1.ship_type_id;

    -- Insert into dim_raster and thereafter fact_cell_heatmap
    FOR heatmap IN SELECT * FROM heatmap_data LOOP
        INSERT INTO dim_raster AS dr (rast, partition_id) VALUES (heatmap.rast, part_id) RETURNING dr.raster_id INTO rast_id;

        INSERT INTO fact_cell_heatmap as fch (cell_x, cell_y, date_id, time_id, ship_type_id, raster_id, heatmap_type_id, spatial_resolution, temporal_resolution_sec, partition_id)
        VALUES (heatmap.cell_x, heatmap.cell_y, date_key, heatmap.time_id, heatmap.ship_type_id, rast_id, heatmap.heatmap_type_id, s_res, t_res, part_id);
    END LOOP;

    -- Just cleanup to be sure
    DROP TABLE heatmap_data;
END;
$$