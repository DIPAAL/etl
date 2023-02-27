CREATE OR REPLACE FUNCTION create_histogram(rast RASTER, spatial_resolution INTEGER, temporal_resolution INTEGER) RETURNS INTEGER AS $$
DECLARE
    histogram_id INTEGER;
BEGIN
	INSERT INTO dim_histogram AS dh (histogram, spatial_resolution, temporal_resolution) VALUES (rast, spatial_resolution, temporal_resolution) RETURNING dh.histogram_id INTO histogram_id;
    RETURN histogram_id;
END;
$$ LANGUAGE plpgsql