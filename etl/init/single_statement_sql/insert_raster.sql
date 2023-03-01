CREATE OR REPLACE FUNCTION insert_raster(rast RASTER, spatial_resolution INTEGER, temporal_resolution INTEGER) RETURNS INTEGER AS $$
DECLARE
    raster_id INTEGER;
BEGIN
    INSERT INTO dim_raster AS dr (rast, spatial_resolution_srid_unit, temporal_resolution_sec) VALUES (rast, spatial_resolution, temporal_resolution) RETURNING dr.raster_id INTO raster_id;
    RETURN raster_id;
END;
$$ LANGUAGE plpgsql