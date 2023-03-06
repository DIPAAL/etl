CREATE TABLE IF NOT EXISTS spatial_partition (
    partition_id SMALLINT PRIMARY KEY,
    geom GEOMETRY NOT NULL
);

CREATE INDEX IF NOT EXISTS spatial_partition_idx ON spatial_partition USING spgist(geom);