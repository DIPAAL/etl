CREATE OR REPLACE FUNCTION calculate_delta_upperbounded(doubles double precision[], upper_bound double precision) RETURNS double precision AS $$
DECLARE
    delta double precision;
    delta_1 double precision;
    delta_2 double precision;
    i integer;
BEGIN
    delta := 0;
	IF array_upper(doubles, 1) < 2 OR array_upper(doubles, 1) IS NULL
	THEN
		RETURN delta;
	END IF;
    FOR i IN 2..array_upper(doubles, 1) LOOP
        -- Try the differences on both sides of upper_bound to find the lowest as the delta
        -- Absolute is inherent from using modulo
        delta_1 := dpmod((doubles[i] - doubles[i-1] + upper_bound), upper_bound);
        delta_2 := dpmod((doubles[i-1] - doubles[i] + upper_bound), upper_bound);
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