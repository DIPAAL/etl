CREATE OR REPLACE FUNCTION get_lowest_json_key(json json) RETURNS text AS $$
BEGIN
    RETURN (SELECT key FROM json_each_text(json) ORDER BY value ASC LIMIT 1);
END;
$$ LANGUAGE plpgsql IMMUTABLE;