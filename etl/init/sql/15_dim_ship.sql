CREATE TABLE dim_ship (
    ship_id serial PRIMARY KEY,
    imo int NOT NULL,
    mmsi int NOT NULL,
    mobile_type text NOT NULL,
    ship_type text,
    ship_name text,
    ship_callsign text,
    a float,
    b float,
    c float,
    d float
);

-- Create index on mmsi
CREATE INDEX dim_ship_mmsi_idx ON dim_ship (mmsi);

-- Create index on imo
CREATE INDEX dim_ship_imo_idx ON dim_ship (imo);

-- Create index on ship_type
CREATE INDEX dim_ship_ship_type_idx ON dim_ship (ship_type);

-- Create index on mobile_type
CREATE INDEX dim_ship_mobile_type_idx ON dim_ship (mobile_type);
