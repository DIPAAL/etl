CREATE OR REPLACE FUNCTION dpmod(dividend double precision, divisor double precision) RETURNS double precision AS $$
BEGIN
	RETURN dividend - (floor(dividend / divisor) * divisor);
END;
$$ LANGUAGE plpgsql IMMUTABLE;