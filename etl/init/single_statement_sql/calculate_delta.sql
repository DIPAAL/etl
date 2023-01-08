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


