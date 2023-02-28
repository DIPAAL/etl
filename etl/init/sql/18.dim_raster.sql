CREATE TABLE IF NOT EXISTS dim_raster (
    raster_id SERIAL PRIMARY KEY,
    rast raster NOT NULL,
    spatial_resolution SMALLINT NOT NULL,
    temporal_resolution SMALLINT NOT NULL
);