-- Create schema staging
CREATE SCHEMA IF NOT EXISTS staging;

-- Create reference grids with SP-GIST index.
CREATE TABLE IF NOT EXISTS staging.cell_{CELL_SIZE}m (
    x integer NOT NULL,
    y integer NOT NULL,
    geom geometry NOT NULL,
    PRIMARY KEY (x, y)
);

INSERT INTO staging.cell_{CELL_SIZE}m (x, y, geom)
SELECT
    i as x,
    j as y,
    geom
FROM
    ST_SquareGrid({CELL_SIZE}, (SELECT geom FROM reference_geometries WHERE type = 'spatial_domain')) AS geom;

CREATE INDEX IF NOT EXISTS cell_{CELL_SIZE}m_geom_idx ON staging.cell_{CELL_SIZE}m USING SPGIST (geom);

SELECT create_reference_table('staging.cell_{CELL_SIZE}m');