CREATE OR REPLACE FUNCTION create_partitions_with_split_vertical (geom geometry, split int, level int)
RETURNS void AS $$
DECLARE
    geom_bottom geometry;
    geom_upper geometry;
    numPoints_bottom int;
    numPoints_upper int;
BEGIN
    SELECT ST_MakeEnvelope(ST_XMin(geom), ST_YMin(geom), ST_XMax(geom), split,3034) INTO geom_bottom;
    SELECT ST_MakeEnvelope(ST_XMin(geom), split, ST_XMax(geom), ST_YMax(geom),3034) INTO geom_upper;

    SELECT (SELECT coalesce(sum(coalesce(value,0)),0) FROM staging.fivek_heatmap fk WHERE ST_Contains(geom_bottom, fk.geom)) INTO numPoints_bottom;
    SELECT (SELECT coalesce(sum(coalesce(value,0)),0) FROM staging.fivek_heatmap fk WHERE ST_Contains(geom_upper, fk.geom)) INTO numPoints_upper;

    INSERT INTO staging.partitions (geom, numPoints, level) VALUES (geom_bottom, numPoints_bottom, level+1);
    INSERT INTO staging.partitions (geom, numPoints, level) VALUES (geom_upper, numPoints_upper, level+1);
END;
$$ LANGUAGE plpgsql;