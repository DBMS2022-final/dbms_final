import datetime


class DataPoint:
    def __init__(self, timestamp: datetime, value: float) -> None:
        self.timestamp = timestamp
        self.value = value

    def strftime(self):
        return self.timestamp.strftime('%Y-%m-%d %H:%M:%S')


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
