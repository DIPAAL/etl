CREATE TABLE dim_cell_50m (
    i integer NOT NULL,
    j integer NOT NULL,
    geom geometry NOT NULL,
    PRIMARY KEY (i, j)
);

-- Create the 50m grid
-- EPSG3034
-- UpperLeft = 3602375,3471675
-- LowerRight = 4392275,3055475
INSERT INTO dim_cell_50m (i, j, geom)
SELECT
    i,
    j,
    geom
FROM
    ST_SquareGrid(5000, ST_SetSRID(ST_MakeBox2D(ST_Point(3602375, 3055475), ST_Point(4392275, 3471675)), 3034)) AS geom;

-- Create spatial index
CREATE INDEX dim_cell_50m_geom_idx ON dim_cell_50m USING gist (geom);
