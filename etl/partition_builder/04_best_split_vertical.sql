CREATE OR REPLACE FUNCTION binary_search_for_best_split_vertical(geom geometry, min_y int, max_y int, points_less_than_y_min int, points_greater_than_y_max int)
RETURNS int AS $$
DECLARE
    split int;
    geom_upper geometry;
    geom_bottom geometry;
    points_upper int;
    points_bottom int;
BEGIN
    -- base case: if the difference between the two sides is less than or equal to 5000, return the side with the most points
    IF (max_y - min_y) <= 5000 THEN
        IF points_less_than_y_min > points_greater_than_y_max THEN
            RETURN min_y;
        ELSE
            RETURN max_y;
        END IF;
    END IF;

    -- try to split the distance between min_y and max_y in half
    split := (min_y + (max_y - min_y)/2) - (max_y - min_y)/2 % 5000;

    geom_upper := ST_MakeEnvelope(ST_XMin(geom), split, ST_XMax(geom), max_y, 3034);
    geom_bottom := ST_MakeEnvelope(ST_XMin(geom), min_y, ST_XMax(geom), split, 3034);

    -- calculate the points on each side of the split
    points_upper := (SELECT coalesce(sum(coalesce(value,0)),0) FROM staging.fivek_heatmap fk WHERE ST_Contains(geom_upper, fk.geom)) + points_greater_than_y_max;
    points_bottom := (SELECT coalesce(sum(coalesce(value,0)),0) FROM staging.fivek_heatmap fk WHERE ST_Contains(geom_bottom, fk.geom)) + points_less_than_y_min;

    -- call recursively on the side with the most points, unless it is the same as the other side
    IF points_bottom = points_upper THEN
        RETURN split;
    ELSEIF points_bottom > points_upper THEN
        RETURN binary_search_for_best_split_vertical(geom, min_y, split, points_less_than_y_min, points_upper);
    ELSE
        RETURN binary_search_for_best_split_vertical(geom, split, max_y, points_bottom, points_greater_than_y_max);
    END IF;
END;
$$ LANGUAGE plpgsql;