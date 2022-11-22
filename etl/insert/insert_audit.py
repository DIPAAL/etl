"""Module responsible for inserting all audit data into the database."""
from etl.constants import GLOBAL_AUDIT_LOGGER
from etl.insert.bulk_inserter import BulkInserter


class AuditInserter(BulkInserter):
    """Class responsible for inserting all audit data into the database.

    Inherits from the BulkInserter class.

    Methods
    -------
    insert_audit(conn): insert all audit data into the database
    """

    def insert_audit(self, conn):
        """Insert all audit data into the database."""
        """"""
        df = GLOBAL_AUDIT_LOGGER.get_logs_db()

        query = """
            INSERT INTO audit_log (
            import_date, import_time, requirements, etl_version,
            file_name, file_size, file_rows, cleaning_delta_time, cleaning_rows,
            spatial_join_delta_time, spatial_join_rows, trajectory_delta_time, trajectory_rows,
            cell_construct_delta_time, cell_construct_rows, bulk_insert_delta_time, bulk_insert_rows,
            total_delta_time
            )
            VALUES {}
            """

        self._bulk_insert(df, conn, query, fetch=False)

        pass
