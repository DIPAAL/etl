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
  i2.end_time AS end_time,
  rg.geom AS geom,
  rg.geom_geodetic as geom_geodetic,
  (SELECT array_agg(ship_type) FROM dim_ship_type ORDER BY random() LIMIT floor(random() * 8 + 2)::integer) AS ship_types -- random between 2 and 10
FROM reference_geometries rg, (
    SELECT
        i1.start_time,
        i1.start_time + random() * (:period_end_timestamp - i1.start_time) AS end_time
    FROM
    (
        SELECT :period_start_timestamp + random() * (:period_end_timestamp - :period_start_timestamp) AS start_time
    ) i1
) i2
WHERE rg.id = (SELECT id from reference_geometries WHERE type = 'enc' ORDER BY random() LIMIT 1)