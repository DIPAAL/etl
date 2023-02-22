CREATE TABLE IF NOT EXISTS dim_ship_type (
    ship_type_id SMALLSERIAL PRIMARY KEY,
    location_system_type TEXT NOT NULL,
    mobile_type TEXT NOT NULL,
    ship_type TEXT NOT NULL,

    UNIQUE(location_system_type, mobile_type, ship_type)
)