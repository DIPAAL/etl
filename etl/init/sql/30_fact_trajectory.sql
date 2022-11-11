CREATE TABLE fact_trajectory (
    ship_id integer NOT NULL,
    ship_junk_id smallint NOT NULL,
    start_date_id integer NOT NULL,
    start_time_id integer NOT NULL,
    end_date_id integer NOT NULL,
    end_time_id integer NOT NULL,
    eta_date_id integer NOT NULL,
    eta_time_id integer NOT NULL,
    nav_status_id smallint NOT NULL,
    trajectory_id integer NOT NULL,
    PRIMARY KEY (ship_id, ship_junk_id, start_date_id, start_time_id, end_date_id, end_time_id, eta_date_id, eta_time_id, nav_status_id, trajectory_id),

    duration interval NOT NULL,
    length int NOT NULL,
    infer_stopped boolean NOT NULL,

    FOREIGN KEY (ship_id) REFERENCES dim_ship(ship_id),
    FOREIGN KEY (ship_junk_id) REFERENCES dim_ship_junk(ship_junk_id),
    FOREIGN KEY (start_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (start_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (end_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (end_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (eta_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (eta_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (nav_status_id) REFERENCES dim_nav_status(nav_status_id),
    FOREIGN KEY (start_date_id, trajectory_id) REFERENCES dim_trajectory(date_id, trajectory_id)
) PARTITION BY RANGE (start_date_id);