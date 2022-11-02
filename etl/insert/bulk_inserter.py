import pandas as pd


class BulkInserter:
    def __init__(self, bulk_size=1000):
        self.bulk_size = bulk_size

    def _bulk_insert(self, entries: pd.DataFrame, conn, query: str, fetch=True) -> pd.Series:
        num_batches = len(entries) // self.bulk_size + 1
        batches = [entries[i * self.bulk_size:(i + 1) * self.bulk_size] for i in range(num_batches)]
        sub_id_series = [self.__insert(batch, conn, query, fetch=fetch) for batch in batches]

        if not fetch:
            return

        return pd.concat(sub_id_series)

    @staticmethod
    def __insert(batch: pd.DataFrame, conn, query: str, fetch: bool) -> pd.Series:
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
