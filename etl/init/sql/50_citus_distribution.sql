-- Reference all dimensions except ships
SELECT create_reference_table('dim_date');
SELECT create_reference_table('dim_time');
SELECT create_reference_table('dim_direction');
SELECT create_reference_table('dim_nav_status');
SELECT create_reference_table('dim_ship_type');
SELECT create_reference_table('dim_ship');

-- Cell dimensions, must be descending order to avoid foreign key constraint violations
SELECT create_reference_table('dim_cell_5000m');
SELECT create_reference_table('dim_cell_1000m');
SELECT create_reference_table('dim_cell_200m');
SELECT create_reference_table('dim_cell_50m');

-- And other references
SELECT create_reference_table('reference_geometries');

-- Distribute the fact tables and trajectory dimension
SELECT create_distributed_table('dim_trajectory', 'trajectory_sub_id');
SELECT create_distributed_table('fact_trajectory', 'trajectory_sub_id', colocate_with=>'dim_trajectory');
SELECT create_distributed_table('fact_cell_1000m', 'trajectory_sub_id', colocate_with=>'dim_trajectory');
SELECT create_distributed_table('fact_cell_5000m', 'trajectory_sub_id', colocate_with=>'dim_trajectory');
SELECT create_distributed_table('fact_cell_200m', 'trajectory_sub_id', colocate_with=>'dim_trajectory');
SELECT create_distributed_table('fact_cell_50m', 'trajectory_sub_id', colocate_with=>'dim_trajectory');


