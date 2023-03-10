CREATE OR REPLACE FUNCTION build_kd_tree(gdgeom geometry, numPartitions int)
    RETURNS TABLE
            (
                partition_id bigint,
                geom         geometry,
                numPoints    int,
                level        int
            )
AS
$$
DECLARE
    maxPoints        int;
    t_partition_id   bigint;
    t_partition_geom geometry;
    t_numPoints      int;
    t_level          int;
    split            int;
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS temp_partitions
    (
        partition_id bigserial,
        geom         geometry,
        numPoints    int,
        level        int
    );
    TRUNCATE temp_partitions;

    -- get number of cell facts in the global domain
    SELECT sum(value) INTO STRICT maxPoints FROM staging.fivek_heatmap;

    -- put the global domain in the partitions table
    INSERT INTO temp_partitions (geom, numPoints, level) VALUES (gdgeom, maxPoints, 1);


    -- while count of partitions is less than maxNumPartitions
    WHILE (SELECT count(*) FROM temp_partitions) < numPartitions
        LOOP
            -- print the amount of partitions currently.
            RAISE NOTICE 'partitions: %', (SELECT count(*) FROM temp_partitions);

            -- get the partition with the most points and not at maxDepth
            SELECT * INTO STRICT t_partition_id, t_partition_geom, t_numPoints, t_level
            FROM temp_partitions p
            WHERE (ST_XMax(p.geom) - ST_XMin(p.geom)) >= 10000
               OR (ST_YMax(p.geom) - ST_YMin(p.geom)) >= 10000
            ORDER BY numPoints DESC LIMIT 1;

            -- delete the partition from the partitions table
            DELETE FROM temp_partitions p WHERE p.partition_id = t_partition_id;

            IF ((t_level % 2) = 0 AND (ST_XMax(t_partition_geom) - ST_XMin(t_partition_geom) >= 10000)) OR
               (ST_YMax(t_partition_geom) - ST_YMin(t_partition_geom)) < 10000 THEN
                SELECT binary_search_for_best_split_horizontal(t_partition_geom, ST_XMin(t_partition_geom)::int, ST_XMax(t_partition_geom)::int,0,0) INTO split;
                PERFORM create_partitions_with_split_horizontal(t_partition_geom, split, t_level);
            ELSE
                SELECT binary_search_for_best_split_vertical(t_partition_geom, ST_YMin(t_partition_geom)::int, ST_YMax(t_partition_geom)::int, 0,0) INTO split;
                PERFORM create_partitions_with_split_vertical(t_partition_geom, split, t_level);
            END IF;

        END LOOP;

    -- return the partitions
    RETURN QUERY SELECT * FROM temp_partitions;
END;
$$ LANGUAGE plpgsql;
