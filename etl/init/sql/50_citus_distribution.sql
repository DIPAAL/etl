-- Reference all dimensions except ships
SELECT create_reference_table('dim_date');
SELECT create_reference_table('dim_time');
SELECT create_reference_table('dim_direction');
SELECT create_reference_table('dim_nav_status');
SELECT create_reference_table('dim_ship_type');
SELECT create_reference_table('dim_ship');

-- And other references
SELECT create_reference_table('spatial_partition');
SELECT create_reference_table('reference_geometries');
SELECT create_reference_table('dim_heatmap_type');

-- Distribute the fact tables and trajectory dimension
SELECT create_distributed_table('dim_trajectory', 'trajectory_sub_id', shard_count=>'400');
SELECT create_distributed_table('fact_trajectory', 'trajectory_sub_id', colocate_with=>'dim_trajectory');

SELECT create_distributed_table('fact_cell_5000m', 'partition_id', shard_count=>'1');
SELECT create_distributed_table('fact_cell_1000m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('fact_cell_200m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('fact_cell_50m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('dim_cell_5000m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('dim_cell_1000m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('dim_cell_200m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('dim_cell_50m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('fact_cell_heatmap', 'partition_id', colocate_with=>'fact_cell_5000m');

-- Create the custom shards
-- Because we ensured colocation be splitting this is done for all colocated tables
WITH parts AS (
    SELECT generate_series(1, 400) part_id
), hashes AS (
    SELECT hashoid(part_id) hash FROM parts ORDER BY hashoid(part_id) LIMIT 399
)
SELECT citus_split_shard_by_split_points(
    -- Shard to split
    (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'fact_cell_5000m'::regclass),
    -- Split points in the hash range
    ARRAY_AGG(hashes.hash::text),
    -- The node ids to put new shards. '%' 4 because we have 4 workers
    (SELECT ARRAY_AGG(part_id % 4 + 1) FROM generate_series(1, 400) part_id),
    -- Determines how shards are transfered 'block_writes' work with 'replica' wal_level and it does not matter during init
    shard_transfer_mode := 'block_writes'
) FROM hashes;