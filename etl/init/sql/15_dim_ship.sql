CREATE TABLE dim_ship (
    ship_id serial PRIMARY KEY,
    imo int NOT NULL,
    mmsi int NOT NULL,
    name text,
    callsign text,
    a float,
    b float,
    c float,
    d float,
    UNIQUE (imo, mmsi, name, callsign, a, b, c, d)
);

-- Create index on mmsi
CREATE INDEX dim_ship_mmsi_idx ON dim_ship (mmsi);

-- Create index on imo
CREATE INDEX dim_ship_imo_idx ON dim_ship (imo);
