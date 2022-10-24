CREATE TABLE dim_ship (
    imo int NOT NULL,
    mmsi int NOT NULL,
    mmsi_subkey int NOT NULL,
    mobile_type text NOT NULL,
    ship_type text,
    ship_name text,
    ship_callsign text,
    a float,
    b float,
    c float,
    d float,

    PRIMARY KEY (mmsi, mmsi_subkey),
    UNIQUE (imo, mmsi, mobile_type, ship_type, ship_name, ship_callsign, a, b, c, d)
);

-- Create index on mmsi
CREATE INDEX dim_ship_mmsi_idx ON dim_ship (mmsi);

-- Create index on imo
CREATE INDEX dim_ship_imo_idx ON dim_ship (imo);

-- Create index on ship_type
CREATE INDEX dim_ship_ship_type_idx ON dim_ship (ship_type);

-- Create index on mobile_type
CREATE INDEX dim_ship_mobile_type_idx ON dim_ship (mobile_type);
