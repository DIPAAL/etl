CREATE TABLE IF NOT EXISTS dim_histogram (
    histogram_id SERIAL PRIMARY KEY,
    histogram raster NOT NULL,
    spatial_resolution SMALLINT NOT NULL,
    temporal_resolution SMALLINT NOT NULL
);