CREATE TABLE fact_grid (
    id bigserial PRIMARY KEY,
    grid_id integer NOT NULL,
    ship_id integer NOT NULL,
    entry_date_id integer NOT NULL,
    entry_time_id integer NOT NULL,
    exit_date_id integer NOT NULL,
    exit_time_id integer NOT NULL,
    direction_id smallint NOT NULL,
    nav_status_id smallint NOT NULL,

    sog float NOT NULL,
    cog float NOT NULL,
    draught float NOT NULL,

    FOREIGN KEY (grid_id) REFERENCES dim_grid_50m(grid_id),
    FOREIGN KEY (ship_id) REFERENCES dim_ship(ship_id),
    FOREIGN KEY (entry_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (entry_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (exit_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (exit_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (direction_id) REFERENCES dim_direction(direction_id),
    FOREIGN KEY (nav_status_id) REFERENCES dim_nav_status(nav_status_id)
);