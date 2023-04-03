"""Module responsible for inserting all audit data into the database."""
from etl.audit.logger import global_audit_logger as gal
from etl.insert.bulk_inserter import BulkInserter


class AuditInserter(BulkInserter):
    """Class responsible for inserting all audit data into the database.

    Inherits from the BulkInserter class.

    Methods
    -------
    insert_audit(conn): insert all audit data into the database
    """

    def insert_audit(self, conn):
        """Insert all audit data into the database.

        Keyword arguments:
            conn: database connection used for insertion
        """
        df = gal.to_dataframe().convert_dtypes()

        query = """
            INSERT INTO audit_log (
                import_datetime, statistics, requirements, etl_version,
                file_name, file_size, date_id, total_delta_time
            )
            VALUES {}
            """

        self._bulk_insert(df, conn, query, fetch=False)
