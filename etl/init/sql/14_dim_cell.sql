CREATE TABLE dim_cell_50m (
    x integer NOT NULL,
    y integer NOT NULL,
    geom geometry NOT NULL,
    PRIMARY KEY (x, y)
    -- Padding: None. Variable length column is last, no padding is needed.
);

-- Create the 50m grid
-- EPSG3034
-- UpperLeft = 3602375,3471675
-- LowerRight = 4392275,3055475
INSERT INTO dim_cell_50m (x, y, geom)
SELECT
    i as x,
    j as y,
    geom
FROM
    ST_SquareGrid(50, ST_SetSRID(ST_MakeBox2D(ST_Point(3602375, 3055475), ST_Point(4392275, 3471675)), 3034)) AS geom;

-- Create spatial index
CREATE INDEX dim_cell_50m_geom_idx ON dim_cell_50m USING spgist (geom);
