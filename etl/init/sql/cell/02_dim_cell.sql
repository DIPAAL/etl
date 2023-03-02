-- Create the {CELL_SIZE}m grid dimension
CREATE TABLE dim_cell_{CELL_SIZE}m (
    x integer NOT NULL,
    y integer NOT NULL,
    parent_x integer,
    parent_y integer,
    geom geometry NOT NULL,
    PRIMARY KEY (x, y)
);

CREATE INDEX dim_cell_{CELL_SIZE}m_geom_idx ON dim_cell_{CELL_SIZE}m USING spgist (geom);

SELECT create_reference_table('dim_cell_{CELL_SIZE}m');