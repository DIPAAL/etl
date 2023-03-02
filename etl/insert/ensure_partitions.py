"""Ensure that the date entry exists and partitions for the given date exists in partitioned tables."""
from etl.helper_functions import extract_date_from_smart_date_id


def ensure_partitions_for_partitioned_tables(conn, date_id: int):
    """
    Ensure that partitions for the given date exists in partitioned tables.

    Args:
        conn: The database connection
        date_id: The smarte date id to ensure exists
    """
    date_partitioned_table_names = [
        "fact_cell_5000m",
        "fact_cell_1000m",
        "fact_cell_200m",
        "fact_cell_50m",
        "fact_trajectory",
        "dim_trajectory",
        "fact_cell_heatmap"
    ]

    for table_name in date_partitioned_table_names:
        _ensure_partition_for_table(conn, table_name, date_id)


def _ensure_partition_for_table(conn, table_name: str, date_id: int):
    """
    Ensure that a partition for the given date exists in the given table.

    Args:
        conn: The database connection
        table_name: The name of the table to ensure a partition exists for
        date_id: The smart date id to ensure exists
    """
    rounded_smart_date_id = date_id - (date_id % 100)

    query = """
        SELECT 1 FROM pg_inherits
        JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child             ON pg_inherits.inhrelid  = child.oid
        WHERE parent.relname = %(relation_name)s
        AND child.relname = %(partition_name)s
    """
    # Partition name is the year and month of the smart date id. The month is 0 padded to 2 digits.
    date = extract_date_from_smart_date_id(date_id)
    partition_name = f"{table_name}_{date.year}_{str(date.month).zfill(2)}"

    cursor = conn.cursor()
    cursor.execute(query, {"relation_name": table_name, "partition_name": partition_name})
    if cursor.fetchone() is None:
        query = f"""
            SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
            CREATE TABLE {partition_name} PARTITION OF {table_name}
                FOR VALUES FROM ({rounded_smart_date_id}) TO ({rounded_smart_date_id + 99})
        """
        cursor.execute(query)
        conn.commit()
