-- Create {0[CELL_SIZE]} fact tables
CREATE TABLE fact_cell_{0[CELL_SIZE]}m (
    st_bounding_box stbox NOT NULL,

    sog float NOT NULL,
    delta_heading float,
    draught float,
    delta_cog float,

    cell_x integer NOT NULL,
    cell_y integer NOT NULL,
    ship_id integer NOT NULL,
    entry_date_id integer NOT NULL,
    entry_time_id integer NOT NULL,
    exit_time_id integer NOT NULL,
    trajectory_sub_id integer NOT NULL,
    direction_id smallint NOT NULL,
    nav_status_id smallint NOT NULL,
    -- Padding: 2+2 = 4 bytes, so 4 bytes of padding to reach the MAXALIGN of 8 bytes

    PRIMARY KEY (cell_x, cell_y, ship_id, entry_date_id, entry_time_id, exit_time_id, direction_id, nav_status_id, trajectory_sub_id),
    FOREIGN KEY (ship_id) REFERENCES dim_ship(ship_id),
    FOREIGN KEY (entry_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (entry_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (exit_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (direction_id) REFERENCES dim_direction(direction_id),
    FOREIGN KEY (nav_status_id) REFERENCES dim_nav_status(nav_status_id),
    FOREIGN KEY (entry_date_id, trajectory_sub_id) REFERENCES dim_trajectory(date_id, trajectory_sub_id)
) PARTITION BY RANGE (entry_date_id);
CREATE INDEX fact_cell_{0[CELL_SIZE]}m_st_bounding_box_idx ON fact_cell_{0[CELL_SIZE]}m USING spgist (st_bounding_box);

SELECT create_distributed_table('fact_cell_{0[CELL_SIZE]}m', 'trajectory_sub_id', colocate_with=>'dim_trajectory');