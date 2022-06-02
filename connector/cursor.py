from mysql.connector.cursor import MySQLCursor

from .compression import DataPoint, Compression


class Cursor(MySQLCursor):
    def __init__(self, connection=None):
        super().__init__(connection)
        self._select_flag = False
        self._selected_row_generator = None

    def execute(self, operation: str, params=None, multi=False):
        if not operation:
            return None

        if not self._connection:
            raise Exception("Cursor is not connected")

        stmt = operation.lower()

        if 'insert' in stmt:
            self._select_flag = False
            return self._custom_insert(stmt)
        elif 'select' in stmt:
            return self._custom_select(stmt)
        else:
            self._select_flag = False
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
        # set self._select_flag = True
        # store interpolated result from select_interpolation to
        # self._selected_row_generator
        return super().execute(stmt)

    def _custom_fetchone(self):
        assert self._select_flag

        next_point = next(self._selected_row_generator)
        return next_point.timestamp, next_point.value

    def _custom_fetchall(self):
        assert self._select_flag

        result = [(pnt.timestamp, pnt.value)
                  for pnt in self._selected_row_generator]
        return result
