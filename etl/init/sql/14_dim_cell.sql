CREATE TABLE dim_cell_50m (
    cell_id serial PRIMARY KEY,
    row int NOT NULL,
    col int NOT NULL,
    geom geometry NOT NULL
);

-- Create the 50m grid
-- EPSG3034
-- UpperLeft = 3602375,3471675
-- LowerRight = 4392275,3055475
INSERT INTO dim_cell_50m (row, col, geom)
SELECT
    i AS row,
    j AS col,
    geom
FROM
    ST_SquareGrid(50, ST_SetSRID(ST_MakeBox2D(ST_Point(3602375, 3055475), ST_Point(4392275, 3471675)), 3034)) AS geom;

-- Create spatial index
CREATE INDEX dim_cell_50m_geom_idx ON dim_cell_50m USING gist (geom);