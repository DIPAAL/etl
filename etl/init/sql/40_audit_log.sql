CREATE TABLE audit_log (
    audit_id serial PRIMARY KEY,
    import_date integer NOT NULL,
    import_time integer NOT NULL,

    file_imported text NOT NULL,
    file_size integer NOT NULL,
    etl_version_major integer NOT NULL,
    etl_version_middle integer NOT NULL,
    etl_version_minor integer NOT NULL,

    -- The following fields store the time it took to perform each step of the ETL process.
    timespan_initial_cleaning integer NOT NULL,
    timespan_spatial_join integer NOT NULL,
    timespan_trajectory_construction integer NOT NULL,
    timespan_bulk_insert integer NOT NULL,
    timespan_total integer NOT NULL,

    -- The following fields store the number of rows after the data has been processed by each step of the ETL process.
    rows_imported integer, -- The number of rows in the input file.
    rows_initial_cleaning integer,
    rows_spatial_join integer,
    rows_trajectory_construction integer,
    rows_bulk_insert integer
)