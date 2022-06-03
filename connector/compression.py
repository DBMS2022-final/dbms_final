from typing import Optional, List

from .data_structure import DataPoint, Buffer


class Compression:
    def __init__(self, dev_margin: float,
                 archieved_point: DataPoint = None,
                 snapshot_point: DataPoint = None) -> None:
        self.dev_margin = dev_margin
        self.buffer = Buffer(archieved_point=archieved_point,
                             snapshot_point=snapshot_point)
        self.slope_min = None
        self.slope_max = None

    def insert_checker(self, new_point: DataPoint) -> Optional[DataPoint]:
        """check slope(new_point, archived point) is safe or not"""
        if not self.buffer.archieved_point:
            self.buffer.push_new_point(new_point)
            return new_point

        if not self.buffer.snapshot_point:
            self.buffer.push_new_point(new_point)
            self._update_slope_interval(new_point)
            return None

        slope_incoming = self._calculate_slope(new_point)

        if self.slope_min <= slope_incoming <= self.slope_max:
            self._update_slope_interval(new_point)
            self.buffer.update_snapshot(new_point)
            return None
        else:
            save_point = self.buffer.save_snapshot(new_point)
            self.slope_min, self.slope_max = None, None
            self._update_slope_interval(new_point)
            return save_point

    def select_interpolation(self, archieved_points: List[DataPoint]):
        assert len(archieved_points) >= 1

        # TODO
        # work as a generator
        if len(archieved_points) == 1:
            if self.buffer.snapshot_point:
                archieved_points.append(self.buffer.snapshot_point)
            else:
                yield archieved_points[0]

        for pnt in archieved_points:
            yield pnt

    def _calculate_slope(self, point: DataPoint, offset=0):
        delta_value = point.value - self.buffer.archieved_point.value + offset
        delta_time = (point.timestamp -
                      self.buffer.archieved_point.timestamp).total_seconds()
        return delta_value / delta_time

    def _update_slope_interval(self, new_point: DataPoint):
        smin_current, smax_current = self._calc_current_slope_interval(
            new_point)
        if self.slope_min:
            self.slope_min = max(self.slope_min, smin_current)
        else:
            self.slope_min = smin_current

        if self.slope_max:
            self.slope_max = min(self.slope_max, smax_current)
        else:
            self.slope_max = smax_current

    def _calc_current_slope_interval(self, new_point: DataPoint):
        smin_current = self._calculate_slope(new_point, -self.dev_margin)
        smax_current = self._calculate_slope(new_point, self.dev_margin)
        return smin_current, smax_current
