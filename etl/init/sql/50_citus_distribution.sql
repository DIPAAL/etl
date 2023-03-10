-- Reference all dimensions except ships
SELECT create_reference_table('dim_date');
SELECT create_reference_table('dim_time');
SELECT create_reference_table('dim_direction');
SELECT create_reference_table('dim_nav_status');
SELECT create_reference_table('dim_cell_50m');
SELECT create_reference_table('dim_ship_junk');
SELECT create_reference_table('dim_ship');

-- And other references
SELECT create_reference_table('danish_waters');

-- Distribute the fact tables and ship dimension
SELECT create_distributed_table('dim_trajectory', 'trajectory_sub_id');
SELECT create_distributed_table('fact_trajectory', 'trajectory_sub_id', colocate_with=>'dim_trajectory');
SELECT create_distributed_table('fact_cell', 'trajectory_sub_id', colocate_with=>'dim_trajectory');


