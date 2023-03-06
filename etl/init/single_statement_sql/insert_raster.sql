CREATE OR REPLACE FUNCTION insert_raster(rast RASTER, partition_id SMALLINT) RETURNS INTEGER AS $$
DECLARE
    raster_id INTEGER;
BEGIN
    INSERT INTO dim_raster AS dr (rast, partition_id) VALUES (rast, partition_id) RETURNING dr.raster_id INTO raster_id;
    RETURN raster_id;
END;
$$ LANGUAGE plpgsql