WITH spatial_bound (xmin, ymin, xmax, ymax, width, height) AS (
    SELECT
            ST_XMin(rg.geom)::integer AS xmin,
            ST_YMin(rg.geom)::integer AS ymin,
            ST_XMax(rg.geom)::integer AS xmax,
            ST_YMax(rg.geom)::integer AS ymax,
            (ST_XMax(rg.geom) - ST_XMin(rg.geom))::integer AS width,
            (ST_YMax(rg.geom) - ST_YMin(rg.geom))::integer AS height
    FROM reference_geometries rg
    WHERE rg.id = (SELECT id from reference_geometries WHERE type = 'enc' ORDER BY random() LIMIT 1)
), reference (rast) AS (
    SELECT ST_AddBand(
        ST_MakeEmptyRaster (bound.width / 5000, bound.height / 5000, (bound.xmin - (bound.xmin % 5000)), (bound.ymin - (bound.ymin % 5000)), 5000, 5000, 0, 0, 3034),
        '32BUI'::text,
        1,
        0
    ) AS rast
    FROM spatial_bound AS bound
)
SELECT
    CASE WHEN q3.rast IS NULL THEN NULL ELSE
        ST_AsGDALRaster(q3.rast,'GTiff')
    END AS raster
FROM (
    SELECT ST_MapAlgebra(q2.rast, reference.rast, '[rast1.val]+[rast2.val]', extenttype := 'SECOND') AS rast
    FROM reference, (
        SELECT ST_Union(q1.rast) AS rast FROM (
            SELECT
                -- If there are 2 bands in the raster, assume it is to calculate average by dividing the first band by the second band
                CASE WHEN ST_Numbands(q0.rast) > 1 THEN
                    ST_MapAlgebra(q0.rast, 1, q0.rast, 2, '[rast1.val]/[rast2.val]', extenttype := 'FIRST')
                ELSE
                    q0.rast
                END AS rast
            FROM (
                SELECT
                    fch.partition_id, ST_Union(fch.rast, (SELECT union_type FROM dim_heatmap_type WHERE slug = 'max_draught')) AS rast
                FROM spatial_bound sb, fact_cell_heatmap fch
                JOIN dim_ship_type dst ON dst.ship_type_id = fch.ship_type_id
                WHERE fch.spatial_resolution = 5000
                AND dst.ship_type IN (SELECT ship_type FROM dim_ship_type ORDER BY random() LIMIT floor(random() * 8 + 2)::integer) -- random between 2 and 10
                AND fch.heatmap_type_id = (SELECT heatmap_type_id FROM dim_heatmap_type WHERE slug = 'max_draught')
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) <= (SELECT '2022-06-01T00:00:00Z'::timestamptz + interval '1 day' * floor(random() * 99 + 1)) -- end_timestamp
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) >= (SELECT '2022-01-01T00:00:00Z'::timestamptz + interval '1 day' * floor(random() * 99 + 1)) -- start_timestamp
                AND fch.cell_x >= sb.xmin / 5000 -- Always 5000
                AND fch.cell_x < sb.xmax / 5000 -- Always 5000
                AND fch.cell_y >= sb.ymin / 5000 -- Always 5000
                AND fch.cell_y < sb.ymax / 5000 -- Always 5000
                AND fch.date_id BETWEEN 20220101 AND 20221231
                GROUP BY fch.partition_id
            ) q0
        ) q1
    ) q2
)q3
;