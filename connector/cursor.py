import datetime
import re
from typing import Dict, Optional

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
            return self._custom_create_table(stmt)
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
        # parse table name, first two column names
        # call compression insert_checkout
        # if return value is not num
        table_name = re.search(r"into\s(\w+)\s", stmt).group(1)

        col_time = 'timestamp'
        col_value = 'value'

        # Ryan
        # parse the value of timestamp and value
        # format of timestamp: '2022-06-02 21:17:01'
        val_pattern = r"values\s+?\('((\w+-*)+\s(\w+:*)+)',\s?(\W?\w+)\)"
        matched = re.search(val_pattern, stmt)
        
        if not matched:
            raise Exception("Insertion should only contain two values")

        time_stamp = matched.group(1)
        val = float(matched.group(4))
        input_time = datetime.datetime.strptime(
            time_stamp, "%Y-%m-%d %H:%M:%S")

        test_point = DataPoint(input_time, val)

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

        store interpolated result from select_interpolation to
        self._selected_row_generator
        """

        """
        should between be handled?
        """
        if ("<" in stmt
                or ">" in stmt
                or "where" not in stmt):
            self._handle_select_many(stmt)
        else:
            self._handle_select_one(stmt)

    def _custom_create_table(self, stmt: str):
        """

        Assume that dev_margin = xxx only appears at the last part
        seperated by a comma
        For example,
        CREATE TABLE temp (
            Id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME,
            value DOUBLE,
            dev_margin=2.5
        );
        """
        table_name = re.search(r"table\s(\w+)", stmt).group(1)

        dev_pattern = r"dev_margin\s?=\s?(\d+(.\d+)?)"
        dev_match = re.search(dev_pattern, stmt)
        if not dev_match:
            return super().execute(stmt)

        dev_value = float(dev_match.group(1))
        self._create_dev_margin_table_if_not_exists()
        self._insert_dev_margin(table_name, dev_value)
        self.compression_dict[table_name] = Compression(
            dev_margin=dev_value)

        previous_comma_position = dev_match.start()
        while previous_comma_position > 0:
            if stmt[previous_comma_position] == ',':
                break
            previous_comma_position -= 1

        modified_stmt = stmt[:previous_comma_position] + stmt[dev_match.end():]
        super().execute(modified_stmt)

    def _custom_fetchone(self):
        assert self._select_flag

        next_point = next(self._selected_row_generator)
        return next_point.timestamp, next_point.value

    def _custom_fetchall(self):
        assert self._select_flag

        result = [(pnt.timestamp, pnt.value)
                  for pnt in self._selected_row_generator]
        return result

    def _handle_select_one(self, stmt):
        table_name = re.search(r"from\s(\w+)", stmt).group(1)

        select_pattern = r"where\s+?timestamp\s+?=\s+?'((\w+-*)+\s(\w+:*)+)'"
        selected_timestamp = re.search(select_pattern, stmt).group(1)
        
        if not selected_timestamp:
            raise ValueError("The format of timestamp should be: 'Y-m-d H:M:S'")

        selected_timestamp = datetime.datetime.strptime(selected_timestamp, "%Y-%m-%d %H:%M:%S")

        """ Query the timestamp to see if it exist in DB """
        stmt_pre_query = (
            f"SELECT timestamp, value" 
            f"FROM {table_name}"
            f"WHERE timestamp = '{selected_timestamp}'"
            )
        
        super().execute(stmt_pre_query)
        result_tuple = super().fetchall()  

        """the asked point does exist in DB"""
        if result_tuple:
            self._selected_row_generator = (x for x in (result_tuple, ))
            return
        
        """the asked point does NOT exist"""
        lower_bound_point = self._get_closest_point('prev', table_name, selected_timestamp)
        upper_bound_point = self._get_closest_point('next', table_name, selected_timestamp)


        comp = self.compression_dict[table_name]
        self._selected_row_generator = comp.select_interpolation(
            selected_timestamp,
            (lower_bound_point, upper_bound_point)
        )

        # TODO if have timeo
        # handle if table_name not in compression_dict.keys()

        ### why would we have to handle the table that does not exist?

    def _handle_select_many(self, stmt):
        # TODO
        self._selected_row_generator = (
            DataPoint(datetime.datetime.now(), -999 * i) for i in range(3))

    def _create_dev_margin_table_if_not_exists(self):
        stmt_creat_table = (
            "CREATE TABLE IF NOT EXISTS dev_margin ("
            "    Id int NOT NULL AUTO_INCREMENT PRIMARY KEY,"
            "    table_name varchar(40),"
            "    dev_margin DOUBLE"
            ")"
        )
        super().execute(stmt_creat_table)

    def _insert_dev_margin(self, table_name, dev_margin):
        stmt_insert_dev = (
            "INSERT INTO dev_margin (table_name, dev_margin) "
            "VALUES (%s, %s);"
        )
        super().execute(stmt_insert_dev, (table_name, dev_margin))
        return

    def _get_closest_point(
            self, prev_or_next: str, 
            table_name: str,
            specified_time: datetime.datetime
            ) -> Optional[DataPoint]:

        if prev_or_next == 'prev':
            compare_sign = '<='   
            sort_order = 'DESC'
        elif prev_or_next == 'next':
            compare_sign = '>='
            sort_order = 'ASC'
        else:
            error_message = (f"prev_or_next should be 'prev' or 'next', "
                             f"get {prev_or_next}")
            raise ValueError(error_message)

        str_time = specified_time.strftime('%Y-%m-%d %H:%M:%S')
        stmt_select_previous_point = (
            f"SELECT timestamp, value FROM {table_name} "
            f"WHERE timestamp {compare_sign} '{str_time}' "
            f"ORDER BY timestamp {sort_order} LIMIT 1"
        )
        super().execute(stmt_select_previous_point)
        result_row = super().fetchone()
        if result_row:
            return DataPoint(*result_row)
        else:
            return None
