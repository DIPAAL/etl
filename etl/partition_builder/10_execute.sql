INSERT INTO dim_partition (geom)
SELECT geom FROM build_kd_tree(
    (SELECT ST_Envelope(geom) FROM reference_geometries WHERE type = 'cleaning_ref'),
    400
);
