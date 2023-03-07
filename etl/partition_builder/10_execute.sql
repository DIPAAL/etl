INSERT INTO spatial_partition (geom)
SELECT geom
FROM build_kd_tree(
    (SELECT ST_Envelope(ST_Union(geom)) FROM staging.cell_5000m),
    400
);
