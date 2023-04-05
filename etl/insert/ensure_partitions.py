"""Ensure that the date entry exists and partitions for the given date exists in partitioned tables."""
from etl.helper_functions import extract_date_from_smart_date_id
from etl.constants import ACCESS_METHOD_HEAP, ACCESS_METHOD_COLUMNAR
from sqlalchemy import Connection, text


def ensure_partitions_for_partitioned_tables(conn, date_id: int):
    """
    Ensure that partitions for the given date exists in partitioned tables.

    Args:
        conn: The database connection
        date_id: The smarte date id to ensure exists
    """
    date_partitioned_table_names = [
        ("fact_cell_5000m", ACCESS_METHOD_HEAP),
        ("fact_cell_1000m", ACCESS_METHOD_HEAP),
        ("fact_cell_200m", ACCESS_METHOD_HEAP),
        ("fact_cell_50m", ACCESS_METHOD_HEAP),
        ("fact_trajectory", ACCESS_METHOD_HEAP),
        ("dim_trajectory", ACCESS_METHOD_HEAP),
        ("fact_cell_heatmap", ACCESS_METHOD_COLUMNAR)
    ]

    for table_name, access_method in date_partitioned_table_names:
        _ensure_partition_for_table(conn, table_name, access_method, date_id)


def _ensure_partition_for_table(conn: Connection, table_name: str, access_method: str, date_id: int):
    """
    Ensure that a partition for the given date exists in the given table.

    Args:
        conn: The database connection
        table_name: The name of the table to ensure a partition exists for
        access_method: The table access method to use
        date_id: The smart date id to ensure exists
    """
    rounded_smart_date_id = date_id - (date_id % 100)

    exist_query = """
        SELECT 1 FROM pg_inherits
        JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child             ON pg_inherits.inhrelid  = child.oid
        WHERE parent.relname = :relation_name
        AND child.relname = :partition_name
    """
    # Partition name is the year and month of the smart date id. The month is 0 padded to 2 digits.
    date = extract_date_from_smart_date_id(date_id)
    partition_name = f"{table_name}_{date.year}_{str(date.month).zfill(2)}"

    cursor = conn.execute(text(exist_query), parameters={"relation_name": table_name, "partition_name": partition_name})
    if cursor.fetchone() is None:
        query = f"""
            SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
            CREATE TABLE {partition_name} PARTITION OF {table_name}
                FOR VALUES FROM ({rounded_smart_date_id}) TO ({rounded_smart_date_id + 99})
                USING {access_method}
        """
        conn.execute(text(query))
        conn.commit()
