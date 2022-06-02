import datetime
import re
from typing import Dict

from mysql.connector.cursor import MySQLCursor

from .compression import Compression
from .data_structure import DataPoint
from .settings import Config


class Cursor(MySQLCursor):
    def __init__(self, connection=None):
        super().__init__(connection)
        self._select_flag = False
        self._selected_row_generator = None
        self.compression_dict: Dict[str, Compression] = {}

    def execute(self, operation: str, params=None, multi=False):
        if not operation:
            return None

        if not self._connection:
            raise Exception("Cursor is not connected")

        stmt = operation.lower()

        self._select_flag = False
        if 'insert' in stmt:
            return self._custom_insert(stmt)
        elif 'select' in stmt:
            self._select_flag = True
            return self._custom_select(stmt)
        elif 'create table' in stmt:
            return self._custum_create_table(stmt)
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
        # parse table name, first two column names
        # call compression insert_checkout
        # if return value is not num
        table_name = re.search(r"into\s(\w+)\s", stmt).group(1)
        col_time = 'timestamp'
        col_value = 'value'

        test_point = DataPoint(datetime.datetime.now(), -999)

        if table_name not in self.compression_dict.keys():
            comp = Compression(dev_margin=Config.DEV_MARGIN,
                               archieved_point=test_point)
            self.compression_dict[table_name] = comp
            point_to_be_saved = test_point
        else:
            comp = self.compression_dict[table_name]
            point_to_be_saved = comp.insert_checker(test_point)

        if point_to_be_saved:
            sql = (f"INSERT INTO {table_name}({col_time}, {col_value})"
                   f"VALUES (%s, %s);")
            super().execute(sql, (point_to_be_saved.strftime(), point_to_be_saved.value))

    def _custom_select(self, stmt: str):
        """

        TODO
        set self._select_flag = True
        store interpolated result from select_interpolation to
        self._selected_row_generator
        """
        table_name = re.search(r"from\s(\w+)", stmt).group(1)

        test_point1 = DataPoint(datetime.datetime.now(), -999)
        test_point2 = DataPoint(
            datetime.datetime.now() + datetime.timedelta(minutes=8), -1999)

        comp = self.compression_dict[table_name]
        self._selected_row_generator = comp.select_interpolation(
            [test_point1, test_point2])
        return

    def _custum_create_table(self, stmt: str):
        # TODO
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
