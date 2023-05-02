WITH reference (rast) AS (
    SELECT ST_AddBand(
        ST_MakeEmptyRaster(
            :width / :spatial_resolution,
            :height / :spatial_resolution,
            (:xmin - (:xmin % :spatial_resolution)),
            (:ymin - (:ymin % :spatial_resolution)),
            :spatial_resolution,
            :spatial_resolution,
            0,
            0,
            3034
        ),
        '32BUI'::text,
        1,
        0
    ) AS rast
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
                    fch.partition_id, ST_Union(fch.rast, (SELECT union_type FROM dim_heatmap_type WHERE slug = 'time')) AS rast
                FROM fact_cell_heatmap fch
                JOIN dim_ship_type dst ON dst.ship_type_id = fch.ship_type_id
                WHERE fch.spatial_resolution = :spatial_resolution
                AND dst.ship_type = ANY (:ship_types) -- random between 2 and 10
                AND fch.heatmap_type_id = (SELECT heatmap_type_id FROM dim_heatmap_type WHERE slug = 'time')
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) <= :end_time -- end_timestamp
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) >= :start_time -- start_timestamp
                AND fch.cell_x >= :xmin / 5000 -- Always 5000
                AND fch.cell_x < :xmax / 5000 -- Always 5000
                AND fch.cell_y >= :ymin / 5000 -- Always 5000
                AND fch.cell_y < :ymax / 5000 -- Always 5000
                AND fch.date_id BETWEEN 20210101 AND 20211231
                GROUP BY fch.partition_id
            ) q0
        ) q1
    ) q2
)q3
;