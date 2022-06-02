from mysql.connector.cursor import MySQLCursor


class Cursor(MySQLCursor):
    def __init__(self, connection=None):
        super().__init__(connection)
        self._select_flag = False
        pass

    def execute(self, operation: str, params=None, multi=False):
        if not operation:
            return None

        if not self._connection:
            raise Exception("Cursor is not connected")

        stmt = operation.lower()

        if 'insert' in stmt:
            return self._custom_insert(stmt)
        elif 'select' in stmt:
            return self._custom_select(stmt)
        else:
            return super().execute(operation, params, multi)

    def fetchone(self):
        if self._select_flag:
            return self._custom_fetchone()
        else:
            return super().fetchone()

    def fetchall(self):
        if self._select_flag:
            return self._custom_fetchall()
        else:
            return super().fetchall()

    def _custom_insert(self, stmt: str):
        # TODO
        return super().execute(stmt)

    def _custom_select(self, stmt: str):
        # TODO
        return super().execute(stmt)

    def _custom_fetchone(self):
        # TODO
        return super().fetchone()

    def _custom_fetchall(self):
        # TODO
        return super().fetchall()
