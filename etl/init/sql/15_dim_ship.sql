CREATE TABLE dim_ship (
    a float,
    b float,
    c float,
    d float,
    ship_id serial PRIMARY KEY,
    imo int NOT NULL,
    mmsi int NOT NULL,
    -- Padding: 8-4 = 4 bytes
    name text,
    callsign text,
    location_system_type text,
    mobile_type text,
    ship_type text,
    position_fixing_device text,
    UNIQUE (imo, mmsi, name, callsign, a, b, c, d, location_system_type, mobile_type, ship_type, position_fixing_device)
    -- Padding: Maybe, at worst 12 bytes. Text attributes are variable length and will round up to nearest 4 bytes.
);

-- Create index on mmsi
CREATE INDEX dim_ship_mmsi_idx ON dim_ship (mmsi);

-- Create index on imo
CREATE INDEX dim_ship_imo_idx ON dim_ship (imo);
