CREATE TABLE IF NOT EXISTS dim_raster (
    raster_id SERIAL PRIMARY KEY,
    temporal_resolution_sec INTEGER NOT NULL,
    spatial_resolution_meter INTEGER NOT NULL,
    rast raster NOT NULL
);