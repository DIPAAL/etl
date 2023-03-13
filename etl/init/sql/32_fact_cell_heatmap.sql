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

    PRIMARY KEY (cell_x, cell_y, date_id, time_id, ship_type_id, heatmap_type_id, partition_id, temporal_resolution_sec, spatial_resolution),
    FOREIGN KEY (cell_x, cell_y, partition_id) REFERENCES dim_cell_5000m (x, y, partition_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (ship_type_id) REFERENCES dim_ship_type(ship_type_id),
    FOREIGN KEY (heatmap_type_id) REFERENCES dim_heatmap_type(heatmap_type_id),
    FOREIGN KEY (partition_id) REFERENCES spatial_partition(partition_id)
) PARTITION BY RANGE(date_id);