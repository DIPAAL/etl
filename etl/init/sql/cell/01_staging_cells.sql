-- Create schema staging
CREATE SCHEMA IF NOT EXISTS staging;

-- Create reference grids for 50m, 200m, 1000m with spgist index on the geom
-- EPSG3034
-- UpperLeft = 3602375,3471675
-- LowerRight = 4392275,3055475

CREATE TABLE IF NOT EXISTS staging.cell_{0[CELL_SIZE]}m (
    x integer NOT NULL,
    y integer NOT NULL,
    geom geometry NOT NULL,
    PRIMARY KEY (x, y)
);

INSERT INTO staging.cell_{0[CELL_SIZE]}m (x, y, geom)
SELECT
    i as x,
    j as y,
    geom
FROM
    ST_SquareGrid({0[CELL_SIZE]}, ST_SetSRID(ST_MakeBox2D(ST_Point(3602375, 3055475), ST_Point(4392275, 3471675)), 3034)) AS geom;

CREATE INDEX IF NOT EXISTS cell_{0[CELL_SIZE]}m_geom_idx ON staging.cell_{0[CELL_SIZE]}m USING SPGIST (geom);

SELECT create_reference_table('staging.cell_{0[CELL_SIZE]}m');