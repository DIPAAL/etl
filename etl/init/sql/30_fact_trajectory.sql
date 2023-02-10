CREATE TABLE fact_trajectory (
    duration interval NOT NULL,

    ship_id integer NOT NULL,
    start_date_id integer NOT NULL,
    start_time_id integer NOT NULL,
    end_date_id integer NOT NULL,
    end_time_id integer NOT NULL,
    eta_date_id integer NOT NULL,
    eta_time_id integer NOT NULL,
    trajectory_sub_id integer NOT NULL,
    PRIMARY KEY (ship_id, start_date_id, start_time_id, end_date_id, end_time_id, eta_date_id, eta_time_id, nav_status_id, trajectory_sub_id),

    length int NOT NULL default -1,
    nav_status_id smallint NOT NULL,
    infer_stopped boolean NOT NULL,
    -- Padding: 4+2+1 = 7 bytes, so 1 byte of padding to reach the MAXALIGN of 8 bytes

    FOREIGN KEY (ship_id) REFERENCES dim_ship(ship_id),
    FOREIGN KEY (start_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (start_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (end_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (end_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (eta_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (eta_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (nav_status_id) REFERENCES dim_nav_status(nav_status_id),
    FOREIGN KEY (start_date_id, trajectory_sub_id) REFERENCES dim_trajectory(date_id, trajectory_sub_id)
) PARTITION BY RANGE (start_date_id);

CREATE INDEX fact_trajectory_start_date_id_trajectory_sub_id_idx ON fact_trajectory (start_date_id, trajectory_sub_id);
