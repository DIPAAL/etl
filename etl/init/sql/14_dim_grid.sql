CREATE TABLE dim_grid_1000m (
    grid_id serial PRIMARY KEY,
    i int NOT NULL,
    j int NOT NULL,
    geom geometry NOT NULL
);

-- Create spatial index
CREATE INDEX dim_grid_1000m_geom_idx ON dim_grid_1000m USING gist (geom);


CREATE TABLE dim_grid_500m (
    grid_id serial PRIMARY KEY,
    i int NOT NULL,
    j int NOT NULL,
    geom geometry NOT NULL,
    supercell_id int REFERENCES dim_grid_1000m (grid_id)
);

-- Create spatial index
CREATE INDEX dim_grid_500m_geom_idx ON dim_grid_500m USING gist (geom);

CREATE TABLE dim_grid_100m (
    grid_id serial PRIMARY KEY,
    i int NOT NULL,
    j int NOT NULL,
    geom geometry NOT NULL,
    supercell_id int REFERENCES dim_grid_500m (grid_id)
);

-- Create spatial index
CREATE INDEX dim_grid_100m_geom_idx ON dim_grid_100m USING gist (geom);

CREATE TABLE dim_grid_50m (
    grid_id serial PRIMARY KEY,
    i int NOT NULL,
    j int NOT NULL,
    geom geometry NOT NULL,
    supercell_id int REFERENCES dim_grid_100m (grid_id)
);

-- Create spatial index
CREATE INDEX dim_grid_50m_geom_idx ON dim_grid_50m USING gist (geom);

-- Create the 50m grid
WITH seawaters AS (
    SELECT ST_Transform(geom, 25832) as geom FROM danish_waters
)
INSERT INTO dim_grid_50m (i, j, geom)
SELECT
    i,
    j,
    geom
FROM
    ST_SquareGrid(50, (SELECT geom FROM seawaters));

-- Create the 100m grid
WITH seawaters AS (
    SELECT ST_Transform(geom, 25832) as geom FROM danish_waters
)
INSERT INTO dim_grid_100m (i, j, geom)
SELECT
    i,
    j,
    geom
FROM
    ST_SquareGrid(100, (SELECT geom FROM seawaters));

-- Create the 500m grid
WITH seawaters AS (
    SELECT ST_Transform(geom, 25832) as geom FROM danish_waters
)
INSERT INTO dim_grid_500m (i, j, geom)
SELECT
    i,
    j,
    geom
FROM
    ST_SquareGrid(500, (SELECT geom FROM seawaters));

-- Create the 1000m grid
WITH seawaters AS (
    SELECT ST_Transform(geom, 25832) as geom FROM danish_waters
)
INSERT INTO dim_grid_1000m (i, j, geom)
SELECT
    i,
    j,
    geom
FROM
    ST_SquareGrid(1000, (SELECT geom FROM seawaters));

-- update relation such that each 500m cell knows which 1000m cell it is a part of
UPDATE dim_grid_500m
SET supercell_id = dim_grid_1000m.grid_id
FROM dim_grid_1000m
WHERE ST_Contains(dim_grid_1000m.geom, dim_grid_500m.geom);

-- same for 100m
UPDATE dim_grid_100m
SET supercell_id = dim_grid_500m.grid_id
FROM dim_grid_500m
WHERE ST_Contains(dim_grid_500m.geom, dim_grid_100m.geom);

-- same for 50m
UPDATE dim_grid_50m
SET supercell_id = dim_grid_100m.grid_id
FROM dim_grid_100m
WHERE ST_Contains(dim_grid_100m.geom, dim_grid_50m.geom);