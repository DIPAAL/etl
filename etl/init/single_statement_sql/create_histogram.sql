CREATE OR REPLACE FUNCTION create_histogram(histogram_size INTEGER, elem_placements INTEGER[], elem_values INTEGER[]) RETURNS INTEGER[] AS $$
DECLARE
	arr INTEGER[];
	cur_placement INTEGER;
BEGIN
	IF ARRAY_NDIMS(elem_placements) > 1 OR ARRAY_NDIMS(elem_values) > 1
	THEN
		RAISE EXCEPTION 'Only 1-dimentional arrays supported';
	ELSIF histogram_size < ARRAY_UPPER(elem_placements, 1) OR histogram_size < ARRAY_UPPER(elem_values, 1)
	THEN
		RAISE EXCEPTION 'Given parameter "histogram_size" is lower than input arrays. "histogram_size"=%, "placements"=%, "values"=%', histogram_size, ARRAY_UPPER(elem_placements, 1), ARRAY_UPPER(elem_values, 1);
	ELSIF ARRAY_UPPER(elem_placements, 1) != ARRAY_UPPER(elem_values, 1)
	THEN 
		RAISE EXCEPTION 'Parameter arrays "elem_placements" and "elem_values" does not have the same size. "placements"=% AND "values"=%', ARRAY_UPPER(elem_placements, 1), ARRAY_UPPER(elem_values, 1);
	END IF;
	arr := ARRAY_FILL(0, ARRAY[histogram_size]);
	cur_placement := 1;

	FOR i IN 1..histogram_size LOOP
		IF elem_placements[cur_placement] = i
		THEN
			arr[i] := elem_values[cur_placement];
			cur_placement := cur_placement + 1;
		END IF;
	END LOOP;
	RETURN arr;
END;
$$ LANGUAGE plpgsql IMMUTABLE;