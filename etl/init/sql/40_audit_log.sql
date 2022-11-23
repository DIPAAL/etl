CREATE TABLE audit_log (
    audit_id serial PRIMARY KEY,
    import_date date NOT NULL,
    import_time time NOT NULL,
    etl_version text,
    requirements text[],

    file_name text,
    file_size integer,
    file_rows integer,

    -- Delta time is the time each stage took to complete in seconds
    -- Row count is the number of rows after being processed by each stage with the exception of bulk insert
    cleaning_delta_time integer,
    cleaning_rows integer,

    spatial_join_delta_time integer,
    spatial_join_rows integer,

    trajectory_delta_time integer,
    trajectory_rows integer,

    cell_construct_delta_time integer,
    cell_construct_rows integer,

    bulk_insert_delta_time integer,
    bulk_insert_rows integer,  -- Summation. All rows inserted into the database using bulk insert

    total_delta_time integer
)