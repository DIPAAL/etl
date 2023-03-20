-- Create heatmap table
CREATE TABLE IF NOT EXISTS fact_cell_heatmap (
    cell_x INTEGER NOT NULL,
    cell_y INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,

    -- Non-additive Measures
    temporal_resolution_sec INTEGER NOT NULL,
    spatial_resolution INTEGER NOT NULL,

    ship_type_id SMALLINT NOT NULL,
    heatmap_type_id SMALLINT NOT NULL,
    partition_id SMALLINT NOT NULL,

    -- Raster "special" measure
    rast raster NOT NULL,

    PRIMARY KEY (heatmap_type_id, spatial_resolution, temporal_resolution_sec, date_id, time_id, ship_type_id, cell_x, cell_y, heatmap_type_id, partition_id)
) PARTITION BY RANGE(date_id);