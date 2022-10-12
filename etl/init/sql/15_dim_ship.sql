CREATE TABLE dim_ship (
    ship_id serial PRIMARY KEY,
    imo int NOT NULL,
    mmsi int NOT NULL,
    mobile_type char(10) NOT NULL,
    ship_type char(20),
    ship_name char(100),
    ship_callsign char(20),
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
