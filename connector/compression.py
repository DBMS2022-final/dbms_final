import datetime
from typing import Optional, Tuple, Generator

from .data_structure import DataPoint, Buffer


class Compression:
    def __init__(self, dev_margin: float,
                 archieved_point: DataPoint = None,
                 snapshot_point: DataPoint = None) -> None:
        self.dev_margin = dev_margin
        self.buffer = Buffer(archieved_point=archieved_point,
                             snapshot_point=snapshot_point)
        self.time_step: Optional[datetime.timedelta] = None
        self.slope_min = None
        self.slope_max = None

    def insert_checker(self, new_point: DataPoint) -> Optional[DataPoint]:
        """check slope(new_point, archived point) is safe or not"""
        if not self.buffer.archieved_point:
            self.buffer.push_new_point(new_point)
            return new_point

        if not self.buffer.snapshot_point:
            self.time_step = (new_point.timestamp -
                              self.buffer.archieved_point.timestamp)
            self.buffer.push_new_point(new_point)
            self._update_slope_interval(new_point)
            return None

        slope_incoming = self._calculate_slope(new_point=new_point)

        if self.slope_min <= slope_incoming <= self.slope_max:
            self._update_slope_interval(new_point)
            self.buffer.update_snapshot(new_point)
            return None
        else:
            save_point = self.buffer.save_snapshot(new_point)
            self.slope_min, self.slope_max = None, None
            self._update_slope_interval(new_point)
            return save_point

    def select_interpolation(self, specified_time,
                             archieved_points: Tuple[DataPoint]
                             ) -> Generator[DataPoint, None, None]:
        """Do interpolation when SELECT

        Work as a generator

        1. if only need one point
        specified_time: datetime.datetime
        archieved_points: a tuple of two Datapoints
        return: generator of DataPoint

        if the next point are not stored in database
        (i.e in buffer or only insert one point), the only one
        point sould also be in a tuple or list.

        2. if a range
        specified_time: a tuple of two datetime.datetime
        archieved_points: a generator function of Datapoints

        Return: generator of DataPoint
        """
        if isinstance(specified_time, datetime.datetime):
            return self._select_one(specified_time, archieved_points)
        else:
            return self._select_many(specified_time, archieved_points)

    def _select_one(self, specified_time: datetime.datetime,
                    archieved_points: Tuple[DataPoint]
                    ) -> Generator[DataPoint, None, None]:

        assert len(archieved_points) >= 1
        if len(archieved_points) == 1:
            if self.buffer.snapshot_point:
                archieved_points.append(self.buffer.snapshot_point)
            else:
                error_message = ("only one point in the database and no data"
                                 " in the buffer are not implemented yet.")
                raise NotImplementedError(error_message)

        if archieved_points[0].timestamp < archieved_points[1].timestamp:
            old_point, new_point = archieved_points[0], archieved_points[1]
        else:
            old_point, new_point = archieved_points[1], archieved_points[0]

        result_point = self._calc_interpolation(
            specified_time, point_start=old_point, point_end=new_point)
        yield result_point

    def _select_many(self, specified_time: Tuple[datetime.datetime],
                     archieved_points: Generator[DataPoint, None, None]
                     ) -> Generator[DataPoint, None, None]:
        assert len(specified_time) == 2
        start_time, end_time = specified_time[0], specified_time[1]

        if not self.time_step:
            error_message = f"time_step({self.time_step}) is not recorded!"
            raise NotImplementedError(error_message)

        if not end_time:
            if not self.buffer.snapshot_point:
                # TODO
                raise NotImplementedError()
            end_time = self.buffer.snapshot_point.timestamp

        def add_point_to_tail_if_needed() -> Generator[DataPoint, None, None]:
            nonlocal archieved_points
            nonlocal end_time
            pnt = None
            for pnt in archieved_points:
                yield pnt
            if pnt and pnt.timestamp < end_time:
                yield self.buffer.snapshot_point

        point_generator = add_point_to_tail_if_needed()

        try:
            point_prev = next(point_generator)
        except StopIteration:
            # No datapoint in the database
            return

        if not start_time:
            start_time = point_prev.timestamp

        if point_prev.timestamp >= start_time:
            working_time = point_prev.timestamp
            yield point_prev
        else:
            working_time = start_time - self.time_step

        for point_next in point_generator:
            while working_time < point_next.timestamp:
                working_time += self.time_step
                if working_time > end_time:
                    # fetch remaining data contained in the generator, or the
                    # next sql execute will raise unread result found error
                    for _ in point_generator:
                        pass
                    return

                point_result = self._calc_interpolation(
                    working_time,
                    point_start=point_prev, point_end=point_next)
                yield point_result

            if working_time != point_next.timestamp:
                working_time = point_next.timestamp
                yield point_next

            point_prev = point_next

    def _calculate_slope(self,
                         new_point: DataPoint, *,
                         old_point: DataPoint = None,
                         offset=0):
        if not old_point:
            old_point = self.buffer.archieved_point

        if new_point == old_point:
            error_message = (
                f"new_point({new_point}) == old_point({old_point}),"
                " cannot calculate slope!!"
            )
            raise ValueError(error_message)

        delta_value = new_point.value - old_point.value + offset
        delta_time = (new_point.timestamp -
                      old_point.timestamp).total_seconds()
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
        smin_current = self._calculate_slope(
            new_point=new_point, offset=-self.dev_margin)
        smax_current = self._calculate_slope(
            new_point=new_point, offset=self.dev_margin)
        return smin_current, smax_current

    def _calc_interpolation(self, specified_time: datetime.datetime,
                            point_start: DataPoint, point_end: DataPoint):
        slope = self._calculate_slope(
            new_point=point_end, old_point=point_start)
        delta_time = (specified_time - point_start.timestamp).total_seconds()
        interpolation_value = slope * delta_time + point_start.value
        result_point = DataPoint(specified_time, interpolation_value)
        return result_point
