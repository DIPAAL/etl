-- Create schema staging
CREATE SCHEMA IF NOT EXISTS staging;

-- Create reference grids for 50m, 200m, 1000m with spgist index on the geom
-- EPSG3034
-- UpperLeft = 3602375,3471675
-- LowerRight = 4392275,3055475

CREATE TABLE IF NOT EXISTS staging.cell_1000m (
    x integer NOT NULL,
    y integer NOT NULL,
    geom geometry NOT NULL,
    PRIMARY KEY (x, y)
);

INSERT INTO staging.cell_1000m (x, y, geom)
SELECT
    i as x,
    j as y,
    geom
FROM
    ST_SquareGrid(1000, ST_SetSRID(ST_MakeBox2D(ST_Point(3602375, 3055475), ST_Point(4392275, 3471675)), 3034)) AS geom;

CREATE INDEX IF NOT EXISTS cell_1000m_geom_idx ON staging.cell_1000m USING SPGIST (geom);

CREATE TABLE IF NOT EXISTS staging.cell_200m (
    x integer NOT NULL,
    y integer NOT NULL,
    geom geometry NOT NULL,
    PRIMARY KEY (x, y)
);

INSERT INTO staging.cell_200m (x, y, geom)
SELECT
    i as x,
    j as y,
    geom
FROM
    ST_SquareGrid(200, ST_SetSRID(ST_MakeBox2D(ST_Point(3602375, 3055475), ST_Point(4392275, 3471675)), 3034)) AS geom;

CREATE INDEX IF NOT EXISTS cell_200m_geom_idx ON staging.cell_200m USING SPGIST (geom);

CREATE TABLE IF NOT EXISTS staging.cell_50m (
    x integer NOT NULL,
    y integer NOT NULL,
    geom geometry NOT NULL,
    PRIMARY KEY (x, y)
);

INSERT INTO staging.cell_50m (x, y, geom)
SELECT
    i as x,
    j as y,
    geom
FROM
    ST_SquareGrid(50, ST_SetSRID(ST_MakeBox2D(ST_Point(3602375, 3055475), ST_Point(4392275, 3471675)), 3034)) AS geom;

CREATE INDEX IF NOT EXISTS cell_50m_geom_idx ON staging.cell_50m USING SPGIST (geom);





-- Make them all reference tables
SELECT create_reference_table('staging.cell_50m');
SELECT create_reference_table('staging.cell_200m');
SELECT create_reference_table('staging.cell_1000m');
