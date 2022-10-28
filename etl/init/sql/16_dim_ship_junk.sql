CREATE TABLE dim_ship_junk
(
    ship_junk_id smallserial PRIMARY KEY,
    location_system_type text,
    mobile_type text,
    ship_type text,
    UNIQUE (location_system_type, mobile_type, ship_type)
)