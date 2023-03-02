CREATE TABLE dim_ship (
    a float,
    b float,
    c float,
    d float,
    ship_id serial PRIMARY KEY,
    imo int NOT NULL,
    mmsi int NOT NULL,
    ship_type_id SMALLINT NOT NULL,
    -- Padding: 8-4 = 4 bytes
    name text,
    callsign text,
    location_system_type TEXT NOT NULL,
    UNIQUE (imo, mmsi, name, callsign, a, b, c, d, ship_type_id, location_system_type),
    FOREIGN KEY (ship_type_id) REFERENCES dim_ship_type(ship_type_id)
    -- Padding: Maybe, at worst 12 bytes. Text attributes are variable length and will round up to nearest 4 bytes.
);

-- Create index on mmsi
CREATE INDEX dim_ship_mmsi_idx ON dim_ship (mmsi);

-- Create index on imo
CREATE INDEX dim_ship_imo_idx ON dim_ship (imo);
