import datetime
from typing import Optional, List


class DataPoint:
    def __init__(self, timestamp: datetime, value: float) -> None:
        self.timestamp = timestamp
        self.value = value


class Buffer:
    def __init__(self, archieved_point: DataPoint = None,
                 snapshot_point: DataPoint = None,
                 incoming_point: DataPoint = None) -> None:
        self.archieved_point = archieved_point
        self.snapshot_point = snapshot_point
        self.incoming_point = incoming_point

    def push_new_point(self, new_point: DataPoint) -> None:
        """Add new point to buffer"""
        if self.archieved_point is None:
            self.archieved_point = new_point
        elif self.snapshot_point is None:
            self.snapshot_point = new_point
        else:
            self.incoming_point = new_point

    def drop_snapshot(self) -> None:
        self.snapshot_point = self.incoming_point

    def save_snapshot(self) -> DataPoint:
        save_point = self.snapshot_point
        self.archieved_point = self.snapshot_point
        self.snapshot_point = self.incoming_point
        return save_point


class Compression:
    def __init__(self, dev_margin: float,
                 archieved_point: DataPoint = None,
                 snapshot_point: DataPoint = None) -> None:
        self.dev_margin = dev_margin
        self.buffer = Buffer(archieved_point=archieved_point,
                             snapshot_point=snapshot_point,
                             incoming_point=None)
        self.slope_max = None
        self.slope_min = None

    def insert_checker(self, new_point: DataPoint) -> Optional[DataPoint]:
        # TODO:
        # check slope(new_point, archived point) is safe or not
        # if slope
        save_point = self.buffer.save_snapshot()
        self.buffer.push_new_point(new_point)
        return save_point

    def select_interpolation(self, archieved_points: List[DataPoint]):
        # TODO
        # work as a generator
        pass

    def _calculate_slope(self, point1: DataPoint,
                         point2: DataPoint, offset=0):
        delta_value = point2.value - point1.value + offset
        delta_time = point2.timestamp - point1.timestamp
        return delta_value / delta_time
