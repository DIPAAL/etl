-- Reference all dimensions except ships
SELECT create_reference_table('dim_date');
SELECT create_reference_table('dim_time');
SELECT create_reference_table('dim_direction');
SELECT create_reference_table('dim_nav_status');
SELECT create_reference_table('dim_grid_50m');

-- And other references
SELECT create_reference_table('danish_waters');

-- Distribute the fact tables and ship dimension
SELECT create_distributed_table('dim_ship', 'mmsi');
SELECT create_distributed_table('fact_trajectory', 'mmsi', colocate_with=>'dim_ship');
SELECT create_distributed_table('fact_grid', 'mmsi', colocate_with=>'dim_ship');


