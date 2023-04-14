WITH spatial_bound (xmin, ymin, xmax, ymax, width, height) AS (
    SELECT
            ST_XMin(rg.geom)::integer AS xmin,
            ST_YMin(rg.geom)::integer AS ymin,
            ST_XMax(rg.geom)::integer AS xmax,
            ST_YMax(rg.geom)::integer AS ymax,
            (ST_XMax(rg.geom) - ST_XMin(rg.geom))::integer AS width,
            (ST_YMax(rg.geom) - ST_YMin(rg.geom))::integer AS height
    FROM reference_geometries rg
    WHERE rg.id = 117
), reference (rast) AS (
    SELECT ST_AddBand(
        ST_MakeEmptyRaster (bound.width / 5000, bound.height / 5000, (bound.xmin - (bound.xmin % 5000)), (bound.ymin - (bound.ymin % 5000)), 5000, 5000, 0, 0, 3034),
        '32BSI'::text,
        1,
        0
    ) AS rast
    FROM spatial_bound AS bound
)
SELECT
    ST_AsGDALRaster(ST_MapAlgebra(r1.rast, r2.rast, expression := '[rast1.val]-[rast2.val]', pixeltype := '32BSI', nodata1expr := '[rast2.val]', nodata2expr := '0-[rast1.val]', nodatanodataval := '0'),'GTiff')
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
                SELECT ST_Union(fch.rast, (SELECT union_type FROM dim_heatmap_type WHERE slug = 'count')) AS rast
                FROM spatial_bound sb, fact_cell_heatmap fch
                JOIN dim_ship_type dst on fch.ship_type_id = dst.ship_type_id
                WHERE fch.spatial_resolution = 5000
                AND fch.heatmap_type_id = (SELECT heatmap_type_id FROM dim_heatmap_type WHERE slug = 'count')
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) <= timestamp_from_date_time_id(20221231, 235959) -- end_timestamp
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) >= timestamp_from_date_time_id(20220101, 0) -- start_timestamp
                AND dst.ship_type = 'Cargo'
                AND fch.cell_x >= sb.xmin / 5000 -- Always 5000
                AND fch.cell_x < sb.xmax / 5000 -- Always 5000
                AND fch.cell_y >= sb.ymin / 5000 -- Always 5000
                AND fch.cell_y < sb.ymax / 5000 -- Always 5000
                AND fch.date_id BETWEEN 20220101 AND 20221231
                GROUP BY fch.partition_id
            ) q0
        ) q1
    ) q2
) r1, (
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
                SELECT ST_Union(fch.rast, (SELECT union_type FROM dim_heatmap_type WHERE slug = 'count')) AS rast
                FROM spatial_bound sb, fact_cell_heatmap fch
                JOIN dim_ship_type dst on fch.ship_type_id = dst.ship_type_id
                WHERE fch.spatial_resolution = 5000
                AND fch.heatmap_type_id = (SELECT heatmap_type_id FROM dim_heatmap_type WHERE slug = 'count')
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) <= timestamp_from_date_time_id(20221231, 235959) -- end_timestamp
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) >= timestamp_from_date_time_id(20220101, 0) -- start_timestamp
                AND dst.ship_type = 'Cargo'
                AND fch.cell_x >= sb.xmin / 5000 -- Always 5000
                AND fch.cell_x < sb.xmax / 5000 -- Always 5000
                AND fch.cell_y >= sb.ymin / 5000 -- Always 5000
                AND fch.cell_y < sb.ymax / 5000 -- Always 5000
                AND fch.date_id BETWEEN 20220101 AND 20221231
                GROUP BY fch.partition_id
            ) q0
        ) q1
    ) q2
) r2