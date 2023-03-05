"""Class implementing bulk insertion of data into a database."""
import pandas as pd
from etl.audit.logger import global_audit_logger as gal, ROWS_KEY


class BulkInserter:
    """
    Class responsible for bulk inserting data into a database.

    Intended to be used as a subclass.

    Attributes
    ----------
    bulk_size: number of rows to insert in a single transaction

    """

    def __init__(self, dimension_name: str, bulk_size: int = 500, id_col_name: str = None):
        """
        Construct an instance of the BulkInserter class.

        Keyword arguments:
            dimension_name: the table name of the dimension being inserted into
            bulk_size: the number of rows to insert in a single transaction (default: 1000)
            id_col_name: the name of the column containing the id of the dimension (default: None)
        """
        self.bulk_size = bulk_size
        self.dimension_name = dimension_name
        self.id_col_name = id_col_name

    def _bulk_select_insert(self, entries: pd.DataFrame, conn, insert_query: str, select_query: str) -> pd.DataFrame:
        """
        Split entries into bulks and use select-insert to ensure existence in database.

        Keyword arguments:
            entries: dataframes containing rows to be inserted
            conn: database connection used for insertion
            insert_query: the query used to insert into the database
            select_query: the query used to select from the database
        """
        num_batches = len(entries) // self.bulk_size + 1
        batches = [entries[i * self.bulk_size:(i + 1) * self.bulk_size] for i in range(num_batches)]
        inserted_data = [self.__select_insert(batch, conn, insert_query, select_query) for batch in batches]

        return pd.concat(inserted_data)

    def __select_insert(self, batch: pd.DataFrame, conn, insert_query: str, select_query: str) -> pd.DataFrame:
        """
        Select matches from batch to get their IDs, then insert the rest.

        Keyword arguments:
            batch: dataframe containing rows for a single batch
            conn: the database connection to use
            insert_query: the query used to insert into the database
            select_query: the query used to select from the database
        """
        # First use the select query to get the ids of the existing entries.
        # Convert to array string notation [[1,2,3],[4,5,6]].
        column_count = batch.shape[1]
        prepared_row = f"({','.join(['%s'] * column_count)})"
        placeholders = f"({','.join([prepared_row] * len(batch))})"
        select_query = select_query.format(placeholders)

        result = pd.read_sql_query(select_query, conn, params=batch.values.flatten())

        # Use the result dataframe to figure out which rows need to be inserted.
        # Merge by the columns in the batch dataframe.
        result = batch.merge(result, on=batch.columns.tolist(), how='left')

        to_insert = result[result[self.id_col_name].isna()]

        # drop the identifier column from the dataframe to insert
        to_insert = to_insert.drop(columns=[self.id_col_name])

        # Insert the rows that need to be inserted.
        if not to_insert.empty:
            inserted_data = self.__insert(to_insert, conn, insert_query, fetch=True)
            # Assign the ids of the inserted rows to the result dataframe joined by the columns in the batch dataframe.
            result = result.merge(inserted_data, on=to_insert.columns.tolist(), how='left')

            # Merge the id column with suffix _x and _y into a single column.
            result[self.id_col_name] = result[self.id_col_name + '_x'].fillna(result[self.id_col_name + '_y'])

        return result

    def _bulk_insert(self, entries: pd.DataFrame, conn, query: str, fetch: bool = True) -> pd.Series:
        """
        Split entries into bulks and insert into database.

        Keyword arguments:
            entries: dataframes containing rows to be inserted
            conn: database connection used for insertion
            query: the query used to insert into the database
            fetch: whether to fetch the result from executing the query (default True)
        """
        num_batches = len(entries) // self.bulk_size + 1
        batches = [entries[i * self.bulk_size:(i + 1) * self.bulk_size] for i in range(num_batches)]
        fetched_dataframe = [self.__insert(batch, conn, query, fetch=fetch) for batch in batches]

        if not fetch:
            return

        return pd.concat(fetched_dataframe)

    def __insert(self, batch: pd.DataFrame, conn, query: str, fetch: bool) -> pd.DataFrame:
        """
        Insert a batch into the database and returns database IDs.

        Keyword arguments:
            batch: dataframe containing rows for a single batch
            conn: the database connection to use
            query: the query used to insert into the database
            fetch: whether to fetch the result from executing the query
        """
        column_count = batch.shape[1]
        prepared_row = f"({','.join(['%s'] * column_count)})"
        placeholders = ','.join([prepared_row] * len(batch))

        query = query.format(placeholders)

        result = None
        if fetch:
            result = pd.read_sql_query(query, conn, params=batch.values.flatten())
        else:
            with conn.cursor() as cursor:
                cursor.execute(query, batch.values.flatten())

        # Log the number of rows inserted in the GAL
        if self.dimension_name not in gal[ROWS_KEY]:
            gal[ROWS_KEY][self.dimension_name] = 0
        gal[ROWS_KEY][self.dimension_name] += len(batch)

        return result
