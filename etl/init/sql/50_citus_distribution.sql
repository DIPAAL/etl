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

-- Distribute the fact tables and trajectory dimension
SELECT create_distributed_table('dim_trajectory', 'trajectory_sub_id');
SELECT create_distributed_table('fact_trajectory', 'trajectory_sub_id', colocate_with=>'dim_trajectory');
SELECT create_distributed_table('fact_cell_5000m', 'partition_id', shard_count=>'400');
SELECT create_distributed_table('fact_cell_1000m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('fact_cell_200m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('fact_cell_50m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('dim_cell_5000m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('dim_cell_1000m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('dim_cell_200m', 'partition_id', colocate_with=>'fact_cell_5000m');
SELECT create_distributed_table('dim_cell_50m', 'partition_id', colocate_with=>'fact_cell_5000m');