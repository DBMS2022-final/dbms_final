import datetime
from typing import Optional, List

from .data_structure import DataPoint, Buffer


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
        assert len(archieved_points) >= 1

        # TODO
        # work as a generator
        if len(archieved_points) == 1:
            if self.buffer.incoming_point:
                archieved_points.append(self.buffer.incoming_point)
            elif self.buffer.snapshot_point:
                archieved_points.append(self.buffer.snapshot_point)
            else:
                yield archieved_points[0]

        for pnt in archieved_points:
            yield pnt

    def _calculate_slope(self, point1: DataPoint,
                         point2: DataPoint, offset=0):
        delta_value = point2.value - point1.value + offset
        delta_time = point2.timestamp - point1.timestamp
        return delta_value / delta_time
