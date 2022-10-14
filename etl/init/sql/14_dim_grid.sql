CREATE TABLE dim_grid_50m (
    grid_id serial PRIMARY KEY,
    row int NOT NULL,
    col int NOT NULL,
    geom geometry NOT NULL
);

-- Create spatial index
CREATE INDEX dim_grid_50m_geom_idx ON dim_grid_50m USING gist (geom);

-- Create the 50m grid
WITH seawaters AS (
    SELECT ST_Transform(geom, 25832) as geom FROM danish_waters
)
INSERT INTO dim_grid_50m (row, col, geom)
SELECT
    i AS row,
    j AS col,
    geom
FROM
    ST_SquareGrid(50, (SELECT geom FROM seawaters));