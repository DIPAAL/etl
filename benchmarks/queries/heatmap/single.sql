WITH spatial_bound (xmin, ymin, xmax, ymax, width, height) AS (
    SELECT
            ST_XMin(rg.geom)::integer AS xmin,
            ST_YMin(rg.geom)::integer AS ymin,
            ST_XMax(rg.geom)::integer AS xmax,
            ST_YMax(rg.geom)::integer AS ymax,
            (ST_XMax(rg.geom) - ST_XMin(rg.geom))::integer AS width,
            (ST_YMax(rg.geom) - ST_YMin(rg.geom))::integer AS height
    FROM reference_geometries rg
    WHERE rg.id = :AREA_ID
), reference (rast) AS (
    SELECT ST_AddBand(
        ST_MakeEmptyRaster (bound.width / :SPATIAL_RESOLUTION, bound.height / :SPATIAL_RESOLUTION, (bound.xmin - (bound.xmin % :SPATIAL_RESOLUTION)), (bound.ymin - (bound.ymin % :SPATIAL_RESOLUTION)), :SPATIAL_RESOLUTION, :SPATIAL_RESOLUTION, 0, 0, 3034),
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
                    fch.partition_id, ST_Union(fch.rast, (SELECT union_type FROM dim_heatmap_type WHERE slug = :HEATMAP_TYPE)) AS rast
                FROM spatial_bound sb, fact_cell_heatmap fch
                JOIN dim_ship_type dst ON dst.ship_type_id = fch.ship_type_id
                WHERE fch.spatial_resolution = 5000
                AND dst.ship_type = ANY(:SHIP_TYPES)
                AND dst.mobile_type = ANY(:MOBILE_TYPES)
                AND fch.heatmap_type_id = (SELECT heatmap_type_id FROM dim_heatmap_type WHERE slug = :HEATMAP_TYPE)
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) <= timestamp_from_date_time_id(:END_ID, 235959) -- end_timestamp
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) >= timestamp_from_date_time_id(:START_ID, 0) -- start_timestamp
                AND fch.cell_x >= sb.xmin / 5000 -- Always 5000
                AND fch.cell_x < sb.xmax / 5000 -- Always 5000
                AND fch.cell_y >= sb.ymin / 5000 -- Always 5000
                AND fch.cell_y < sb.ymax / 5000 -- Always 5000
                AND fch.date_id BETWEEN :START_ID AND :END_ID
                GROUP BY fch.partition_id
            ) q0
        ) q1
    ) q2
)q3
;