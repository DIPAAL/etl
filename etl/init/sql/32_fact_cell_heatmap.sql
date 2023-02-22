CREATE TABLE IF NOT EXISTS fact_cell_heatmap_50m (
    cell_x INTEGER NOT NULL,
    cell_y INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    ship_type_id INTEGER NOT NULL,
    density_histogram INTEGER[24] NOT NULL,
    PRIMARY KEY (cell_x, cell_y, date_id, ship_type_id),
    FOREIGN KEY (cell_x, cell_y) REFERENCES dim_cell_50m(x, y),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (ship_type_id) REFERENCES dim_ship_type(ship_type_id)
);