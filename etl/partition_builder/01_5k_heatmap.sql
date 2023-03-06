-- Create a table that stores a 5000x5000m heatmap resolution to use for fast partitioning
CREATE TABLE IF NOT EXISTS staging.fivek_heatmap (
    i int, j int,
    value bigint,
    geom geometry(Polygon, 3034),
    PRIMARY KEY (i, j)
);

TRUNCATE TABLE staging.fivek_heatmap;

INSERT INTO staging.fivek_heatmap (i, j, value, geom)
SELECT cell_x i, cell_y j, count(*) as value, st_bounding_box::geometry
FROM fact_cell_5000m
GROUP BY cell_x, cell_y;

-- create spgist index on geom
CREATE INDEX ON staging.fivek_heatmap USING SPGIST (geom);