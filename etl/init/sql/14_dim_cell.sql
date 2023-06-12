-- Create the 5000m grid dimension
CREATE TABLE dim_cell_5000m (
    x integer NOT NULL,
    y integer NOT NULL,
    parent_x integer,
    parent_y integer,
    division_id SMALLINT NOT NULL,
    geom geometry NOT NULL,

    PRIMARY KEY (x, y, division_id),
    FOREIGN KEY (division_id) REFERENCES spatial_division(division_id)
);

CREATE INDEX dim_cell_5000m_geom_idx ON dim_cell_5000m USING spgist (geom);

-- Create the 1000m grid dimension
CREATE TABLE dim_cell_1000m (
    x integer NOT NULL,
    y integer NOT NULL,
    parent_x integer NOT NULL,
    parent_y integer NOT NULL,
    division_id SMALLINT NOT NULL,
    geom geometry NOT NULL,

    PRIMARY KEY (x, y, division_id),
    FOREIGN KEY (parent_x, parent_y, division_id) REFERENCES dim_cell_5000m(x,y, division_id),
    FOREIGN KEY (division_id) REFERENCES spatial_division(division_id)
);

CREATE INDEX dim_cell_1000m_geom_idx ON dim_cell_1000m USING spgist (geom);

-- Create the 200m grid dimension
CREATE TABLE dim_cell_200m (
    x integer NOT NULL,
    y integer NOT NULL,
    parent_x integer NOT NULL,
    parent_y integer NOT NULL,
    division_id SMALLINT NOT NULL,
    geom geometry NOT NULL,

    PRIMARY KEY (x, y, division_id),
    FOREIGN KEY (parent_x, parent_y, division_id) REFERENCES dim_cell_1000m(x,y, division_id),
    FOREIGN KEY (division_id) REFERENCES spatial_division(division_id)
);

CREATE INDEX dim_cell_200m_geom_idx ON dim_cell_200m USING spgist (geom);

-- Create the 50m grid dimension
CREATE TABLE dim_cell_50m (
    x integer NOT NULL,
    y integer NOT NULL,
    parent_x integer NOT NULL,
    parent_y integer NOT NULL,
    division_id SMALLINT NOT NULL,
    geom geometry NOT NULL,

    PRIMARY KEY (x, y, division_id),
    FOREIGN KEY (parent_x, parent_y, division_id) REFERENCES dim_cell_200m(x,y, division_id),
    FOREIGN KEY (division_id) REFERENCES spatial_division(division_id)
);

CREATE INDEX dim_cell_50m_geom_idx ON dim_cell_50m USING spgist (geom);
