import datetime
import re
from typing import Dict, List

from mysql.connector.cursor import MySQLCursor

from .compression import Compression
from .data_structure import DataPoint
from .settings import Config
from . import stmt_parser


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

        stmt = stmt_parser.preprocessing(operation)

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
        # TODO
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

        time_stamp = matched.group(1)
        val = matched.group(4)
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
        # TODO
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
        stmt_preprocess = stmt_parser.preprocessing(stmt)
        table_name = re.search(r"table\s(\w+)", stmt_preprocess).group(1)

        dev_pattern = r"dev_margin\s?=\s?(\d+(.\d+)?)"
        dev_match = re.search(dev_pattern, stmt_preprocess)
        if not dev_match:
            return super().execute(stmt_preprocess)

        dev_value = float(dev_match.group(1))
        self._create_dev_margin_table_if_not_exists()
        self._insert_dev_margin(table_name, dev_value)
        self.compression_dict[table_name] = Compression(
            dev_margin=dev_value)

        previous_comma_position = dev_match.start()
        while previous_comma_position > 0:
            if stmt_preprocess[previous_comma_position] == ',':
                break
            previous_comma_position -= 1

        modified_stmt = stmt_preprocess[:previous_comma_position] + \
            stmt_preprocess[dev_match.end():]
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
        # TODO
        table_name = re.search(r"from\s(\w+)", stmt).group(1)

        test_point1 = DataPoint(datetime.datetime.now(), -999)
        test_point2 = DataPoint(
            datetime.datetime.now() + datetime.timedelta(minutes=8), -1999)

        # TODO if have time
        # handle if table_name not in compression_dict.keys()
        comp = self.compression_dict[table_name]
        self._selected_row_generator = comp.select_interpolation(
            datetime.datetime.now(),
            (test_point1, test_point2)
        )

    def _handle_select_many(self, stmt: str):
        """Handle select with a time range

        case 0(no_limit): no time-related limit in where or no where clause
        SELECT * FROM table_name
        SELECT (timestamp, value) FROM table_name  

        case 1(range): WHERE with both left and right
        SELECT * FROM table_name
        WHERE timestamp >= '2022-06-01 08:30:01'
        AND timestamp < '2022-06-05 21:07:11';

        case 2(after): no left limit
        SELECT (timestamp, value) FROM table_name
        WHERE timestamp > '2022-06-01 08:30:01';

        case 3(before): no right limit
        SELECT (timestamp, value) FROM table_name
        WHERE timestamp <= '2022-06-05 21:07:11';
        """
        stmt_split_where = stmt.split("where")
        if len(stmt_split_where) > 2:
            raise ValueError(f"Multiple where in {stmt}")

        if len(stmt_split_where) == 1:  # case 0
            return self._handle_select_no_time_limit(stmt)

        stmt_after_where = stmt_split_where[1].strip()
        time_conditions = stmt_parser.find_time_condition(stmt_after_where)
        assert len(time_conditions) != 0
        if len(time_conditions) > 2:
            error_message = ("complex where clause with more than "
                             "2 conditions about time is not support")
            raise NotImplementedError(error_message)

        if len(time_conditions) == 2:  # case 1
            return self._handle_select_range(stmt, time_conditions)

        if "<" in time_conditions:  # case 2
            return self._handle_select_before(stmt, time_conditions[0])
        else:  # case 3
            return self._handle_select_after(stmt, time_conditions[0])

    def _handle_select_no_time_limit(self, stmt: str):
        # TODO: find the earliest time in database
        self._selected_row_generator = (
            DataPoint(datetime.datetime.now(), -999 * i) for i in range(3))

    def _handle_select_after(self, stmt: str, time_condition: str):
        # TODO
        self._selected_row_generator = (
            DataPoint(datetime.datetime.now(), -999 * i) for i in range(3))

    def _handle_select_before(self, stmt: str, time_condition: str):
        # TODO: find the earliest time in database
        self._selected_row_generator = (
            DataPoint(datetime.datetime.now(), -999 * i) for i in range(3))

    def _handle_select_range(self, stmt: str, time_conditions: List[str]):
        # TODO
        self._selected_row_generator = (
            DataPoint(datetime.datetime.now(), -999 * i) for i in range(3))

    def _get_previous_saved_point(self, specified_time):
        pass

    def _get_next_saved_point(self, specified_time):
        pass

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
