-- Use inheritance to create 50m, 200m, 1000m, 5000m fact tables
CREATE TABLE fact_cell_50m (
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
CREATE INDEX fact_cell_50m_st_bounding_box_idx ON fact_cell_50m USING spgist (st_bounding_box);

CREATE TABLE fact_cell_200m (
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

    PRIMARY KEY (cell_x, cell_y, ship_id, trajectory_sub_id, entry_date_id, entry_time_id),
    FOREIGN KEY (ship_id) REFERENCES dim_ship(ship_id),
    FOREIGN KEY (entry_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (entry_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (exit_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (direction_id) REFERENCES dim_direction(direction_id),
    FOREIGN KEY (nav_status_id) REFERENCES dim_nav_status(nav_status_id),
    FOREIGN KEY (entry_date_id, trajectory_sub_id) REFERENCES dim_trajectory(date_id, trajectory_sub_id)
) PARTITION BY RANGE (entry_date_id);
CREATE INDEX fact_cell_200m_st_bounding_box_idx ON fact_cell_200m USING spgist (st_bounding_box);

CREATE TABLE fact_cell_1000m (
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

    PRIMARY KEY (cell_x, cell_y, ship_id, trajectory_sub_id, entry_date_id, entry_time_id),
    FOREIGN KEY (ship_id) REFERENCES dim_ship(ship_id),
    FOREIGN KEY (entry_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (entry_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (exit_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (direction_id) REFERENCES dim_direction(direction_id),
    FOREIGN KEY (nav_status_id) REFERENCES dim_nav_status(nav_status_id),
    FOREIGN KEY (entry_date_id, trajectory_sub_id) REFERENCES dim_trajectory(date_id, trajectory_sub_id)
) PARTITION BY RANGE (entry_date_id);
CREATE INDEX fact_cell_1000m_st_bounding_box_idx ON fact_cell_1000m USING spgist (st_bounding_box);

CREATE TABLE fact_cell_5000m (
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

    PRIMARY KEY (cell_x, cell_y, ship_id, trajectory_sub_id, entry_date_id, entry_time_id),
    FOREIGN KEY (ship_id) REFERENCES dim_ship(ship_id),
    FOREIGN KEY (entry_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (entry_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (exit_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (direction_id) REFERENCES dim_direction(direction_id),
    FOREIGN KEY (nav_status_id) REFERENCES dim_nav_status(nav_status_id),
    FOREIGN KEY (entry_date_id, trajectory_sub_id) REFERENCES dim_trajectory(date_id, trajectory_sub_id)
) PARTITION BY RANGE (entry_date_id);
CREATE INDEX fact_cell_5000m_st_bounding_box_idx ON fact_cell_5000m USING spgist (st_bounding_box);
