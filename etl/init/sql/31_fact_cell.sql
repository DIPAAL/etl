CREATE TABLE fact_cell (
    cell_id integer NOT NULL,
    ship_id integer NOT NULL,
    ship_junk_id smallint NOT NULL,
    entry_date_id integer NOT NULL,
    entry_time_id integer NOT NULL,
    exit_date_id integer NOT NULL,
    exit_time_id integer NOT NULL,
    direction_id smallint NOT NULL,
    nav_status_id smallint NOT NULL,
    trajectory_id integer NOT NULL,
    PRIMARY KEY (cell_id, ship_id, ship_junk_id, entry_date_id, entry_time_id, exit_date_id, exit_time_id, direction_id, nav_status_id, trajectory_id),

    sog float NOT NULL,
    cog float NOT NULL,
    draught float NOT NULL,

    FOREIGN KEY (cell_id) REFERENCES dim_cell_50m(cell_id),
    FOREIGN KEY (ship_id) REFERENCES dim_ship(ship_id),
    FOREIGN KEY (ship_junk_id) REFERENCES dim_ship_junk(ship_junk_id),
    FOREIGN KEY (entry_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (entry_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (exit_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (exit_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (direction_id) REFERENCES dim_direction(direction_id),
    FOREIGN KEY (nav_status_id) REFERENCES dim_nav_status(nav_status_id),
    FOREIGN KEY (trajectory_id) REFERENCES dim_trajectory(trajectory_id)
) PARTITION BY RANGE (entry_date_id);