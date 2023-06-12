SELECT
    CASE WHEN q1.rast IS NULL THEN NULL ELSE
        ST_AsGDALRaster(q1.rast,'COG')
    END AS raster
FROM (
    SELECT
        -- If there are 2 bands in the raster, assume it is to calculate average by dividing the first band by the second band
        CASE WHEN ST_Numbands(q0.rast) > 1 THEN
            ST_MapAlgebra(q0.rast, 1, q0.rast, 2, '[rast1.val]/[rast2.val]', extenttype := 'FIRST')
        ELSE
            q0.rast
        END AS rast
    FROM (
        SELECT
            ST_Union(fch.rast, (SELECT union_type FROM dim_heatmap_type WHERE slug = :heatmap_slug)) AS rast
        FROM fact_heatmap fch
        JOIN dim_ship_type dst ON dst.ship_type_id = fch.ship_type_id
        WHERE fch.spatial_resolution = :spatial_resolution
        AND dst.ship_type = ANY (:ship_types)
        AND fch.heatmap_type_id = (SELECT heatmap_type_id FROM dim_heatmap_type WHERE slug = :heatmap_slug)
        AND timestamp_from_date_time_id(fch.date_id, fch.time_id) <= :end_time -- end_timestamp
        AND timestamp_from_date_time_id(fch.date_id, fch.time_id) >= :start_time -- start_timestamp
        AND fch.cell_x >= :xmin / 5000 -- Always 5000
        AND fch.cell_x < :xmax / 5000 -- Always 5000
        AND fch.cell_y >= :ymin / 5000 -- Always 5000
        AND fch.cell_y < :ymax / 5000 -- Always 5000
        AND fch.date_id BETWEEN :start_date_id AND :end_date_id
        GROUP BY fch.division_id
    ) q0
)q1
;