-- Create 50m pre-aggregated heatmap table
CREATE TABLE IF NOT EXISTS fact_cell_heatmap_50m (
    cell_x INTEGER NOT NULL,
    cell_y INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    ship_type_id INTEGER NOT NULL,
    density_histogram INTEGER[24] NOT NULL,
    PRIMARY KEY (cell_x, cell_y, date_id, ship_type_id)
) PARTITION BY RANGE(date_id);

-- Create 200m pre-aggregated heatmap table
CREATE TABLE IF NOT EXISTS fact_cell_heatmap_200m (
    cell_x INTEGER NOT NULL,
    cell_y INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    ship_type_id INTEGER NOT NULL,
    density_histogram INTEGER[24] NOT NULL,
    PRIMARY KEY (cell_x, cell_y, date_id, ship_type_id)
) PARTITION BY RANGE(date_id);

-- Create 1000m pre-aggregated heatmap table
CREATE TABLE IF NOT EXISTS fact_cell_heatmap_1000m (
    cell_x INTEGER NOT NULL,
    cell_y INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    ship_type_id INTEGER NOT NULL,
    density_histogram INTEGER[24] NOT NULL,
    PRIMARY KEY (cell_x, cell_y, date_id, ship_type_id)
) PARTITION BY RANGE(date_id);

-- Create 5000m pre-aggregated heatmap table
CREATE TABLE IF NOT EXISTS fact_cell_heatmap_5000m (
    cell_x INTEGER NOT NULL,
    cell_y INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    ship_type_id INTEGER NOT NULL,
    density_histogram INTEGER[24] NOT NULL,
    PRIMARY KEY (cell_x, cell_y, date_id, ship_type_id)
) PARTITION BY RANGE(date_id);