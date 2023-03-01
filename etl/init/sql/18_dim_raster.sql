CREATE TABLE IF NOT EXISTS dim_raster (
    raster_id SERIAL PRIMARY KEY,
    rast raster NOT NULL
);