"""Exports constants used in the project."""
from enum import Enum

# Common constants
TRAJECTORY_SRID = 4326
COORDINATE_REFERENCE_SYSTEM = f'epsg:{TRAJECTORY_SRID}'
CVS_TIMESTAMP_FORMAT = '%d/%m/%Y %H:%M:%S'  # 07/09/2021 00:00:00
INT32_MAX = 2147483647
UNKNOWN_INT_VALUE = -1
UNKNOWN_STRING_VALUE = 'Unknown'

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
MOBILE_TYPE_COL = 'Type of mobile'
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
T_TRAJECTORY_SUB_ID_COL = 'trajectory_sub_id'
T_SHIP_NAVIGATIONAL_STATUS_ID_COL = 'ship_navigational_status_id'
T_POSITION_FIXING_DEVICE_COL = 'position_fixing_device'
T_SHIP_TYPE_ID_COL = 'ship_type_id'

# Other dataframe columns
MBDB_TRAJECTORY_COL = 'tgeompoint'
GEO_PANDAS_GEOMETRY_COL = 'geometry'
MID_COL = 'mid'

# Audit log constants - Valid ETL stage names
ETL_STAGE_CLEAN = 'cleaning'
ETL_STAGE_SPATIAL = 'spatial_join'
ETL_STAGE_TRAJECTORY = 'trajectory'
ETL_STAGE_CELL = 'cell_construct'
ETL_STAGE_BULK = 'bulk_insert'

# Database constants
ACCESS_METHOD_HEAP = 'heap'
ACCESS_METHOD_COLUMNAR = 'columnar'
COLUMNAR_TABLE_NAMES = ['fact_cell_heatmap']


# Connection Constants
class SqlalchemyIsolationLevel(Enum):
    """Enum representing sqlalchemy connection isolation levels."""

    def __str__(self) -> str:
        """Return value as string representation."""
        return self.value

    SERIALIZABLE = "SERIALIZABLE"
    REPEATABLE_READ = "REPEATABLE READ"
    READ_COMMITTED = "READ COMMITTED"
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    AUTOCOMMIT = "AUTOCOMMIT"
