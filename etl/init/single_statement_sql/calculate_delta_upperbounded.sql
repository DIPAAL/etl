CREATE OR REPLACE FUNCTION calculate_delta_upperbounded(doubles double precision[], upper_bound double precision) RETURNS double precision AS $$
DECLARE
    delta double precision;
    delta_1 double precision;
    delta_2 double precision;
    i integer;
BEGIN
    delta := 0;
    FOR i IN 2..array_upper(doubles, 1) LOOP
        -- Try the differences on both sides of upper_bound to find the lowest as the delta
        delta_1 := ABS((doubles[i] - doubles[i-1]) - upper_bound);
        delta_2 := ABS((doubles[i-1] - doubles[i]) - upper_bound);
        IF delta_1 < delta_2
        THEN
            delta := delta + delta_1;
        ELSE
            delta := delta + delta_2;
        END IF;
    END LOOP;
    RETURN delta;
END;
$$ LANGUAGE plpgsql IMMUTABLE;