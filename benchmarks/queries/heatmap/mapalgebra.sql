SELECT
    ST_AsGDALRaster(ST_MapAlgebra(r1.rast, r2.rast, expression := '[rast1.val]-[rast2.val]', pixeltype := '32BSI', nodata1expr := '[rast2.val]', nodata2expr := '0-[rast1.val]', nodatanodataval := '0'),'Cog')
FROM (
    SELECT
        -- If there are 2 bands in the raster, assume it is to calculate average by dividing the first band by the second band
        CASE WHEN ST_Numbands(q0.rast) > 1 THEN
            ST_MapAlgebra(q0.rast, 1, q0.rast, 2, '[rast1.val]/[rast2.val]', extenttype := 'FIRST')
        ELSE
            q0.rast
        END AS rast
    FROM (
        SELECT ST_Union(fch.rast, (SELECT union_type FROM dim_heatmap_type WHERE slug = :HEATMAP_TYPE)) AS rast
        FROM fact_cell_heatmap fch
        JOIN dim_ship_type dst on fch.ship_type_id = dst.ship_type_id
        WHERE fch.spatial_resolution = :SPATIAL_RESOLUTION
        AND fch.heatmap_type_id = (SELECT heatmap_type_id FROM dim_heatmap_type WHERE slug = :HEATMAP_TYPE)
        AND timestamp_from_date_time_id(fch.date_id, fch.time_id) <= timestamp_from_date_time_id(:END_ID, 235959) -- end_timestamp
        AND timestamp_from_date_time_id(fch.date_id, fch.time_id) >= timestamp_from_date_time_id(:START_ID, 0) -- start_timestamp
        AND dst.ship_type = ANY(:SHIP_TYPES)
        AND dst.mobile_type = ANY(:MOBILE_TYPES)
        AND fch.cell_x >= :XMIN / 5000 -- Always 5000
        AND fch.cell_x < :XMAX / 5000 -- Always 5000
        AND fch.cell_y >= :YMIN / 5000 -- Always 5000
        AND fch.cell_y < :YMAX / 5000 -- Always 5000
        AND fch.date_id BETWEEN :START_ID AND :END_ID
        GROUP BY fch.partition_id
    ) q0
) r1, (
    SELECT
        -- If there are 2 bands in the raster, assume it is to calculate average by dividing the first band by the second band
        CASE WHEN ST_Numbands(q0.rast) > 1 THEN
            ST_MapAlgebra(q0.rast, 1, q0.rast, 2, '[rast1.val]/[rast2.val]', extenttype := 'FIRST')
        ELSE
            q0.rast
        END AS rast
    FROM (
        SELECT ST_Union(fch.rast, (SELECT union_type FROM dim_heatmap_type WHERE slug = :HEATMAP_TYPE)) AS rast
        FROM fact_cell_heatmap fch
        JOIN dim_ship_type dst on fch.ship_type_id = dst.ship_type_id
        WHERE fch.spatial_resolution = :SPATIAL_RESOLUTION
        AND fch.heatmap_type_id = (SELECT heatmap_type_id FROM dim_heatmap_type WHERE slug = :HEATMAP_TYPE)
        AND timestamp_from_date_time_id(fch.date_id, fch.time_id) <= timestamp_from_date_time_id(:END_ID, 235959) -- end_timestamp
        AND timestamp_from_date_time_id(fch.date_id, fch.time_id) >= timestamp_from_date_time_id(:START_ID, 0) -- start_timestamp
        AND dst.ship_type = ANY(:SHIP_TYPES)
        AND dst.mobile_type = ANY(:MOBILE_TYPES)
        AND fch.cell_x >= :XMIN / 5000 -- Always 5000
        AND fch.cell_x < :XMAX / 5000 -- Always 5000
        AND fch.cell_y >= :YMIN / 5000 -- Always 5000
        AND fch.cell_y < :YMAX / 5000 -- Always 5000
        AND fch.date_id BETWEEN :START_ID AND :END_ID
        GROUP BY fch.partition_id
    ) q0
) r2