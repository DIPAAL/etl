CREATE TABLE dim_ship (
    a float, -- 8 bytes
    b float,
    c float,
    d float,
    ship_id serial PRIMARY KEY,
    imo int NOT NULL, -- Aligned
    mmsi int NOT NULL,
    -- Padding: 4 bytes
    name text,
    callsign text,
    location_system_type text,
    mobile_type text,
    ship_type text,
    UNIQUE (imo, mmsi, name, callsign, a, b, c, d, location_system_type, mobile_type, ship_type)
    -- Padding: Text columns round to nearest 4 bytes at the right boundary.
    -- So worst case is 3 bytes of padding per text column except for the last one.
);

-- Create index on mmsi
CREATE INDEX dim_ship_mmsi_idx ON dim_ship (mmsi);

-- Create index on imo
CREATE INDEX dim_ship_imo_idx ON dim_ship (imo);
