-- Create or replace function that accepts an array of doubles, and calculates the total delta between the entries
CREATE OR REPLACE FUNCTION calculate_delta(doubles double precision[]) RETURNS double precision AS $$
DECLARE
    delta double precision;
    i integer;
BEGIN
    delta := 0;
    -- Iterate over the array and calculate the delta between each entry
    FOR i IN 2..array_upper(doubles, 1) LOOP
        delta := delta + (doubles[i] - doubles[i-1]);
    END LOOP;
    RETURN delta;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Test with:
-- SELECT calculate_delta(ARRAY[1.0]);

-- Create or replace immutable function that returns the json key with the lowest value
CREATE OR REPLACE FUNCTION get_lowest_json_key(json json) RETURNS text AS $$
BEGIN
    RETURN (SELECT key FROM json_each_text(json) ORDER BY value ASC LIMIT 1);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Test with:
-- SELECT get_lowest_json_key('{"a": 1, "b": 2, "c": 3}'::json);
