CREATE TABLE fact_trajectory (
    mmsi integer NOT NULL,
    mmsi_subkey integer NOT NULL,
    start_date_id integer NOT NULL,
    start_time_id integer NOT NULL,
    end_date_id integer NOT NULL,
    end_time_id integer NOT NULL,
    eta_date_id integer NOT NULL,
    eta_time_id integer NOT NULL,
    nav_status_id smallint NOT NULL,
    PRIMARY KEY (mmsi, mmsi_subkey, start_date_id, start_time_id, end_date_id, end_time_id, eta_date_id, eta_time_id, nav_status_id),

    duration interval NOT NULL,
    trajectory tgeompoint NOT NULL,
    infer_stopped boolean NOT NULL,
    destination text NOT NULL,
    rot tfloat NOT NULL,
    heading tfloat NOT NULL,
    draught float NOT NULL,

    FOREIGN KEY (mmsi, mmsi_subkey) REFERENCES dim_ship(mmsi, mmsi_subkey),
    FOREIGN KEY (start_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (start_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (end_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (end_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (eta_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (eta_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (nav_status_id) REFERENCES dim_nav_status(nav_status_id)
) PARTITION BY RANGE (start_date_id);