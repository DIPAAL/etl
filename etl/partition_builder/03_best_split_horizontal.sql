CREATE OR REPLACE FUNCTION binary_search_for_best_split_horizontal(geom geometry, min_x int, max_x int, points_less_than_x_min int, points_greater_than_x_max int)
RETURNS int AS $$
DECLARE
    try_split int;
    geom_left geometry;
    geom_right geometry;
    points_left int;
    points_right int;
BEGIN
    -- base case: if the difference between the two sides is less than or equal to 5000, return the side with the most points
    IF (max_x - min_x) <= 5000 THEN
        IF points_less_than_x_min > points_greater_than_x_max THEN
            RETURN min_x;
        ELSE
            RETURN max_x;
        END IF;
    END IF;

    -- try to split the distance between min_x and max_x in half
    try_split := (min_x + (max_x - min_x)/2) - (max_x - min_x)/2 % 5000;

    geom_left := ST_MakeEnvelope(min_x, ST_YMin(geom), try_split, ST_YMax(geom), 3034);
    geom_right := ST_MakeEnvelope(try_split, ST_YMin(geom), max_x, ST_YMax(geom), 3034);

    -- calculate the points on each side of the split
    points_left := (SELECT coalesce(sum(coalesce(value,0)),0) FROM staging.fivek_heatmap fk WHERE ST_Contains(geom_left, fk.geom)) + points_less_than_x_min;
    points_right := (SELECT coalesce(sum(coalesce(value,0)),0) FROM staging.fivek_heatmap fk WHERE ST_Contains(geom_right, fk.geom)) + points_greater_than_x_max;

    -- call recursively on the side with the most points, unless it is the same as the other side
    IF points_left = points_right THEN
        RETURN try_split;
    ELSEIF points_left > points_right THEN
        RETURN binary_search_for_best_split_horizontal(geom, min_x, try_split, points_less_than_x_min, points_right);
    ELSE
        RETURN binary_search_for_best_split_horizontal(geom, try_split, max_x, points_left, points_greater_than_x_max);
    END IF;
END;
$$ LANGUAGE plpgsql;