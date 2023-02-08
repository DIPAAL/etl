CREATE TABLE fact_cell (
    st_bounding_box stbox NOT NULL, -- 80 bytes

    sog float NOT NULL, -- 8 bytes
    delta_heading float,
    draught float,
    delta_cog float,

    cell_x integer NOT NULL, -- 4 bytes
    cell_y integer NOT NULL,
    ship_id integer NOT NULL,
    entry_date_id integer NOT NULL,
    entry_time_id integer NOT NULL,
    exit_date_id integer NOT NULL,
    exit_time_id integer NOT NULL,
    trajectory_sub_id integer NOT NULL,
    direction_id smallint NOT NULL, -- 2 bytes
    nav_status_id smallint NOT NULL,
    -- Padding: 80 - (4 * 8 + 8 * 4 + 2 * 2) = 80 - 72 = 8 bytes
    PRIMARY KEY (cell_x, cell_y, ship_id, entry_date_id, entry_time_id, exit_date_id, exit_time_id, direction_id, nav_status_id, trajectory_sub_id),


    FOREIGN KEY (cell_x, cell_y) REFERENCES dim_cell_50m(x,y),
    FOREIGN KEY (ship_id) REFERENCES dim_ship(ship_id),
    FOREIGN KEY (entry_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (entry_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (exit_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (exit_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (direction_id) REFERENCES dim_direction(direction_id),
    FOREIGN KEY (nav_status_id) REFERENCES dim_nav_status(nav_status_id),
    FOREIGN KEY (entry_date_id, trajectory_sub_id) REFERENCES dim_trajectory(date_id, trajectory_sub_id)
) PARTITION BY RANGE (entry_date_id);

-- Use SP gist as it is non overlappping.
CREATE INDEX fact_cell_st_bounding_box_idx ON fact_cell USING spgist (st_bounding_box);
