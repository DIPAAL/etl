-- Create heatmap table
CREATE TABLE IF NOT EXISTS fact_cell_heatmap (
    cell_x INTEGER NOT NULL,
    cell_y INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,
    histogram_id INTEGER NOT NULL,
    ship_type_id SMALLINT NOT NULL,
    heatmap_type_id SMALLINT NOT NULL,

    PRIMARY KEY (cell_x, cell_y, date_id, time_id, ship_type_id, histogram_id, heatmap_type_id),
    FOREIGN KEY (cell_x, cell_y) REFERENCES dim_cell_5000m (x, y),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (ship_type_id) REFERENCES dim_ship_type(ship_type_id),
    FOREIGN KEY (histogram_id) REFERENCES dim_histogram(histogram_id),
    FOREIGN KEY (heatmap_type_id) REFERENCES dim_heatmap_type(heatmap_type_id)
) PARTITION BY RANGE(date_id);