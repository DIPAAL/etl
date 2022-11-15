from etl.audit.logger import AuditLogger

# Common constants
TRAJECTORY_SRID = 4326
COORDINATE_REFERENCE_SYSTEM = f'epsg:{TRAJECTORY_SRID}'
CVS_TIMESTAMP_FORMAT = '%d/%m/%Y %H:%M:%S'  # 07/09/2021 00:00:00

# AIS data column names
TIMESTAMP_COL = '# Timestamp'
ETA_COL = 'ETA'
LONGITUDE_COL = 'Longitude'
LATITUDE_COL = 'Latitude'
CALLSIGN_COL = 'Callsign'
CARGO_TYPE_COL = 'Cargo type'
DESTINATION_COL = 'Destination'
NAME_COL = 'Name'
MMSI_COL = 'MMSI'
DRAUGHT_COL = 'Draught'
WIDTH_COL = 'Width'
LENGTH_COL = 'Length'
SOG_COL = 'SOG'
NAVIGATIONAL_STATUS_COL = 'Navigational status'
ROT_COL = 'ROT'
HEADING_COL = 'Heading'
IMO_COL = 'IMO'
COG_COL = 'COG'
POSITION_FIXING_DEVICE_COL = 'Type of position fixing device'
SHIP_TYPE_COL = 'Ship type'
LOCATION_SYSTEM_TYPE_COL = 'Data source type'
A_COL = 'A'
B_COL = 'B'
C_COL = 'C'
D_COL = 'D'

# Trajectory dataframe columns (T=trajectory)
T_INFER_STOPPED_COL = 'infer_stopped'
T_DURATION_COL = 'duration'
T_START_DATE_COL = 'start_date_id'
T_START_TIME_COL = 'start_time_id'
T_END_DATE_COL = 'end_date_id'
T_END_TIME_COL = 'end_time_id'
T_ETA_DATE_COL = 'eta_date_id'
T_ETA_TIME_COL = 'eta_time_id'
T_IMO_COL = 'imo'
T_MMSI_COL = 'mmsi'
T_DRAUGHT_COL = 'draught'
T_A_COL = 'a'
T_B_COL = 'b'
T_C_COL = 'c'
T_D_COL = 'd'
T_NAVIGATIONAL_STATUS_COL = 'nav_status'
T_TRAJECTORY_COL = 'trajectory'
T_DESTINATION_COL = 'destination'
T_ROT_COL = 'rot'
T_HEADING_COL = 'heading'
T_MOBILE_TYPE_COL = 'mobile_type'
T_SHIP_TYPE_COL = 'ship_type'
T_SHIP_NAME_COL = 'ship_name'
T_SHIP_CALLSIGN_COL = 'ship_callsign'
T_SHIP_ID_COL = 'ship_id'
T_LOCATION_SYSTEM_TYPE_COL = 'location_system_type'
T_SHIP_JUNK_ID_COL = 'ship_junk_id'
T_SHIP_TRAJECTORY_ID_COL = 'ship_trajectory_id'
T_SHIP_NAVIGATIONAL_STATUS_ID_COL = 'ship_navigational_status_id'
T_LENGTH_COL = 'length'

# Other dataframe columns
MBDB_TRAJECTORY_COL = 'tgeompoint'
GEO_PANDAS_GEOMETRY_COL = 'geometry'

# Version numbering, currently only used for the audit table in the database.
DICT_VERSION = {'ETL' : 'v1.0.0'}  # Increment this when making changes

# Global audit logger class object, to store the audit log in memory until it is written to the database.
GLOBAL_AUDIT_LOGGER = AuditLogger()
GLOBAL_AUDIT_LOGGER.log_version(DICT_VERSION)
