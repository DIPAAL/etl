WITH bounds (xmin, ymin, xmax, ymax, width, height) AS (
    SELECT
            ST_XMin(rg.geom)::integer AS xmin,
            ST_YMin(rg.geom)::integer AS ymin,
            ST_XMax(rg.geom)::integer AS xmax,
            ST_YMax(rg.geom)::integer AS ymax,
            (ST_XMax(rg.geom) - ST_XMin(rg.geom))::integer AS width,
            (ST_YMax(rg.geom) - ST_YMin(rg.geom))::integer AS height,
            CASE
                WHEN ST_Area(rg.geom) >= 20000000000 THEN 5000
                WHEN ST_Area(rg.geom) >=  2000000000 THEN 1000
                WHEN ST_Area(rg.geom) >=   200000000 THEN 200
                ELSE 50
            END AS spatial_resolution,
            i2.start_time AS start_time,
            i2.end_time AS end_time
    FROM reference_geometries rg, (
        SELECT
            i1.start_time,
            i1.start_time + random() * ('2021-12-31T00:00:00Z'::timestamptz - i1.start_time) AS end_time
        FROM
        (
            SELECT '2021-01-01T00:00:00Z'::timestamptz + random() * ('2021-12-31T00:00:00Z'::timestamptz - '2021-01-01T00:00:00Z'::timestamptz) AS start_time
        ) i1
    ) i2
    WHERE rg.id = (SELECT id from reference_geometries WHERE type = 'enc' ORDER BY random() LIMIT 1)
), reference (rast) AS (
    SELECT ST_AddBand(
        ST_MakeEmptyRaster(
            bound.width / bound.spatial_resolution,
            bound.height / bound.spatial_resolution,
            (bound.xmin - (bound.xmin % bound.spatial_resolution)),
            (bound.ymin - (bound.ymin % bound.spatial_resolution)),
            bound.spatial_resolution,
            bound.spatial_resolution,
            0,
            0,
            3034
        ),
        '32BUI'::text,
        1,
        0
    ) AS rast
    FROM bounds AS bound
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
                    fch.partition_id, ST_Union(fch.rast, (SELECT union_type FROM dim_heatmap_type WHERE slug = 'delta_heading')) AS rast
                FROM bounds b, fact_cell_heatmap fch
                JOIN dim_ship_type dst ON dst.ship_type_id = fch.ship_type_id
                WHERE fch.spatial_resolution = b.spatial_resolution
                AND dst.ship_type IN (SELECT ship_type FROM dim_ship_type ORDER BY random() LIMIT floor(random() * 8 + 2)::integer) -- random between 2 and 10
                AND fch.heatmap_type_id = (SELECT heatmap_type_id FROM dim_heatmap_type WHERE slug = 'delta_heading')
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) <= b.end_time -- end_timestamp
                AND timestamp_from_date_time_id(fch.date_id, fch.time_id) >= b.start_time -- start_timestamp
                AND fch.cell_x >= b.xmin / 5000 -- Always 5000
                AND fch.cell_x < b.xmax / 5000 -- Always 5000
                AND fch.cell_y >= b.ymin / 5000 -- Always 5000
                AND fch.cell_y < b.ymax / 5000 -- Always 5000
                AND fch.date_id BETWEEN 20210101 AND 20211231
                GROUP BY fch.partition_id
            ) q0
        ) q1
    ) q2
)q3
;