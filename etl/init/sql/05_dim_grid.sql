CREATE TABLE dim_grid (
    grid_id serial PRIMARY KEY,
    size int NOT NULL,
    geom geometry NOT NULL,
    is_subcell_of int
    FOREIGN KEY (is_subcell_of) REFERENCES dim_grid (grid_id)
);

-- Create square grid of different sizes in EPSG:3034

-- Points surrounding Denmark
-- xmin = 3750000
-- ymin = 2910000
-- xmax = 4430000
-- ymax = 3580000

-- 50m
INSERT INTO dim_grid (size, geom)
SELECT ST_SquareGrid(50, 0, 0, 1000, 1000);