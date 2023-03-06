CREATE TABLE IF NOT EXISTS dim_raster (
    raster_id SERIAL NOT NULL,
    partition_id SMALLINT NOT NULL,
    rast raster NOT NULL,

    UNIQUE(raster_id),
    PRIMARY KEY (raster_id, partition_id),
    FOREIGN KEY (partition_id) REFERENCES spatial_partition(partition_id)
);