CREATE OR REPLACE FUNCTION insert_raster(rast RASTER) RETURNS INTEGER AS $$
DECLARE
    raster_id INTEGER;
BEGIN
    INSERT INTO dim_raster AS dr (rast) VALUES (rast) RETURNING dr.raster_id INTO raster_id;
    RETURN raster_id;
END;
$$ LANGUAGE plpgsql