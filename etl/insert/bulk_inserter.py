import pandas as pd


class BulkInserter:
    """
    Class responsible for bulk inserting data into a database.
    Intended to be used as a subclass.

    Attributes
    ----------
    bulk_size: number of rows to insert in a single transaction

    """
    def __init__(self, bulk_size: int=1000):
        """
        Constructs an instance of the BulkInserter class.

        bulk_size: number of rows to insert in a single transaction
        """
        self.bulk_size = bulk_size

    def _bulk_insert(self, entries: pd.DataFrame, conn, query: str, fetch: bool=True) -> pd.Series:
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
        sub_id_series = [self.__insert(batch, conn, query, fetch=fetch) for batch in batches]

        if not fetch:
            return

        return pd.concat(sub_id_series)

    @staticmethod
    def __insert(batch: pd.DataFrame, conn, query: str, fetch: bool) -> pd.Series:
        """
        Inserts a batch into the database and returns database IDs.

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
        cursor = conn.cursor()
        cursor.execute(query, batch.values.flatten())
        if not fetch:
            return

        ids = [row[0] for row in cursor.fetchall()]
        return pd.Series(ids, index=batch.index, dtype='int64')
