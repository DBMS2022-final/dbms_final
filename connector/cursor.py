import datetime
import re
from typing import Dict, List, Optional

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
        """Handle insert statement if need compression

        1. Parse table name, first two column names
        2. Call compression insert_checkout
        3. If return value is not None, save the return value
           to database
        """
        table_name = re.search(r"into\s(\w+)\s", stmt).group(1)

        col_time = 'timestamp'
        col_value = 'value'

        # Ryan
        # parse the value of timestamp and value
        # format of timestamp: '2022-06-02 21:17:01'
        # val_pattern = r"values\s+?\('((\w+-*)+\s(\w+:*)+)',\s?(\W?\w+)\)"
        val_pattern = r"values\s+?\('((\w+-*)+\s(\w+:*)+)',\s?(-?\d+(.\d+)?)\)"
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
            value DOUBLE dev_margin=2.5
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

        modified_stmt = (stmt_preprocess[:dev_match.start()] +
                         stmt_preprocess[dev_match.end():])
        super().execute(modified_stmt)

    def _custom_fetchone(self):
        assert self._select_flag

        try:
            next_point = next(self._selected_row_generator)
        except StopIteration:
            return None

        return next_point.timestamp, next_point.value

    def _custom_fetchall(self):
        assert self._select_flag

        result = [(pnt.timestamp, pnt.value)
                  for pnt in self._selected_row_generator]
        return result

    def _handle_select_one(self, stmt):
        """Save selected value as a generator to self._selected_row_generator

        TODO if have time
        handle if table_name not in compression_dict.keys()

        why would we have to handle the table that does not exist?
        case 1: table that do not need compression should be operated
                as normal table as the original version of mysql
        case 2: the connector was closed and reconnected, compression
                objected should be load from extra information table
        """
        table_name = re.search(r"from\s(\w+)", stmt).group(1)

        select_pattern = r"where\s+?timestamp\s+?=\s+?'((\w+-*)+\s(\w+:*)+)'"
        selected_timestamp = re.search(select_pattern, stmt).group(1)

        if not selected_timestamp:
            raise ValueError(
                "The format of query should be: ...where timestamp = 'Y-m-d H:M:S'")

        selected_timestamp = datetime.datetime.strptime(
            selected_timestamp, "%Y-%m-%d %H:%M:%S")

        """ Query the timestamp to see if it exist in DB """
        stmt_pre_query = (
            f"SELECT timestamp, value "
            f"FROM {table_name} "
            f"WHERE timestamp = '{selected_timestamp}'"
        )

        super().execute(stmt_pre_query)
        result_tuple = super().fetchone()

        """the asked point does exist in DB"""
        if result_tuple:
            self._selected_row_generator = (
                x for x in (DataPoint(*result_tuple), ))
            return

        """the asked point does NOT exist"""
        lower_bound_point = self._get_closest_point(
            'prev', table_name, selected_timestamp)
        if not lower_bound_point:
            # select timestamp before the earliest data
            self._selected_row_generator = (_ for _ in [])
            return

        upper_bound_point = self._get_closest_point(
            'next', table_name, selected_timestamp)

        comp = self.compression_dict[table_name]
        self._selected_row_generator = comp.select_interpolation(
            selected_timestamp,
            (lower_bound_point, upper_bound_point)
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

        table_name = stmt_parser.get_table_name_from_select(stmt)
        if len(stmt_split_where) == 1:  # case 0
            return self._handle_select_no_time_limit(table_name)

        stmt_after_where = stmt_split_where[1].strip()
        time_conditions = stmt_parser.find_time_condition(stmt_after_where)
        assert len(time_conditions) != 0
        if len(time_conditions) > 2:
            error_message = ("complex where clause with more than "
                             "2 conditions about time is not support")
            raise NotImplementedError(error_message)

        if len(time_conditions) == 2:  # case 1
            return self._handle_select_range(table_name, time_conditions)

        if "<" in time_conditions[0]:  # case 2
            return self._handle_select_before(table_name, time_conditions[0])
        else:  # case 3
            return self._handle_select_after(table_name, time_conditions[0])

    def _handle_select_no_time_limit(self, table_name: str):
        stmt_select_no_time_limit = (
            f"SELECT timestamp, value FROM {table_name} "
            "ORDER BY timestamp ASC")
        super().execute(stmt_select_no_time_limit)
        points_generator = self._generator_from_super_class_fetchone(
            prev_point=None, next_point=None)

        comp = self.compression_dict[table_name]
        self._selected_row_generator = comp.select_interpolation(
            [None, None], points_generator)

    def _handle_select_after(self, table_name: str, time_condition: str):
        str_start = stmt_parser.get_first_time_from_string(time_condition)
        time_start = datetime.datetime.strptime(
            str_start, '%Y-%m-%d %H:%M:%S')
        prev_point = self._get_closest_point('prev', table_name, time_start)

        stmt_select_after = (
            f"SELECT timestamp, value FROM {table_name} WHERE "
            + time_condition + " ORDER BY timestamp ASC")
        super().execute(stmt_select_after)

        # The case (prev_point is None) is handled by
        # self._generator_from_super_class_fetchone and
        # compression._select_many
        points_generator = self._generator_from_super_class_fetchone(
            prev_point=prev_point, next_point=None)

        comp = self.compression_dict[table_name]
        self._selected_row_generator = comp.select_interpolation(
            [time_start, None], points_generator)

    def _handle_select_before(self, table_name: str, time_condition: str):
        str_end = stmt_parser.get_first_time_from_string(time_condition)
        time_end = datetime.datetime.strptime(
            str_end, '%Y-%m-%d %H:%M:%S')
        next_point = self._get_closest_point('next', table_name, time_end)

        stmt_select_before = (
            f"SELECT timestamp, value FROM {table_name} WHERE "
            + time_condition + " ORDER BY timestamp ASC")
        super().execute(stmt_select_before)

        # The case (next_point is None) is handled by
        # self._generator_from_super_class_fetchone and
        # compression._select_many
        points_generator = self._generator_from_super_class_fetchone(
            prev_point=None, next_point=next_point)

        comp = self.compression_dict[table_name]
        self._selected_row_generator = comp.select_interpolation(
            [None, time_end], points_generator)

    def _handle_select_range(self, table_name: str, time_conditions: List[str]):
        assert len(time_conditions) == 2

        str_start = stmt_parser.get_first_time_from_string(time_conditions[0])
        time_start = datetime.datetime.strptime(
            str_start, '%Y-%m-%d %H:%M:%S')
        prev_point = self._get_closest_point('prev', table_name, time_start)

        str_end = stmt_parser.get_first_time_from_string(time_conditions[1])
        time_end = datetime.datetime.strptime(
            str_end, '%Y-%m-%d %H:%M:%S')
        next_point = self._get_closest_point('next', table_name, time_end)

        stmt_range = (
            f"SELECT timestamp, value FROM {table_name} "
            f"  WHERE {time_conditions[0]}"
            f"  AND {time_conditions[1]}"
            f"  ORDER BY timestamp ASC"
        )
        super().execute(stmt_range)

        points_generator = self._generator_from_super_class_fetchone(
            prev_point=prev_point, next_point=next_point)

        comp = self.compression_dict[table_name]
        self._selected_row_generator = comp.select_interpolation(
            [time_start, time_end], points_generator)

    def _generator_from_super_class_fetchone(self, prev_point: DataPoint,
                                             next_point: DataPoint):
        if prev_point:
            yield prev_point
        row = super().fetchone()
        while row is not None:
            yield DataPoint(*row)
            row = super().fetchone()
        if next_point:
            yield next_point

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

    def _get_closest_point(self, prev_or_next: str,
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
