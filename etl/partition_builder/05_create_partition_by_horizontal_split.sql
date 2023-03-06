CREATE OR REPLACE FUNCTION create_partitions_with_split_horizontal (geom geometry, split int, level int)
RETURNS void AS $$
DECLARE
    geom_left geometry;
    geom_right geometry;
    numPoints_left int;
    numPoints_right int;
BEGIN
    SELECT ST_MakeEnvelope(ST_XMin(geom), ST_YMin(geom), split, ST_YMax(geom),3034) INTO geom_left;
    SELECT ST_MakeEnvelope(split, ST_YMin(geom), ST_XMax(geom), ST_YMax(geom),3034) INTO geom_right;

    SELECT (SELECT coalesce(sum(coalesce(value,0)),0) FROM staging.fivek_heatmap fk WHERE ST_Contains(geom_left, fk.geom)) INTO numPoints_left;
    SELECT (SELECT coalesce(sum(coalesce(value,0)),0) FROM staging.fivek_heatmap fk WHERE ST_Contains(geom_right, fk.geom)) INTO numPoints_right;

    INSERT INTO staging.partitions (geom, numPoints, level) VALUES (geom_right, numPoints_right, level+1);
    INSERT INTO staging.partitions (geom, numPoints, level) VALUES (geom_left, numPoints_left, level+1);
END;
$$ LANGUAGE plpgsql;