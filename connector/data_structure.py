import datetime


class DataPoint:
    def __init__(self, timestamp: datetime, value: float) -> None:
        self.timestamp = timestamp
        self.value = value

    def strftime(self):
        return self.timestamp.strftime('%Y-%m-%d %H:%M:%S')

    def __repr__(self) -> str:
        return f"({self.strftime()}, {self.value})"

    def __eq__(self, other) -> bool:
        if (self.timestamp == other.timestamp
                and abs(self.value - other.value) < 1e-5):
            return True
        else:
            return False


class Buffer:
    def __init__(self, archieved_point: DataPoint = None,
                 snapshot_point: DataPoint = None) -> None:
        self.archieved_point = archieved_point
        self.snapshot_point = snapshot_point

    def __repr__(self) -> str:
        return (f"archieved: {self.archieved_point}\n"
                f"snapshot: {self.snapshot_point}")

    def push_new_point(self, new_point: DataPoint) -> None:
        """Add new point to buffer"""
        if self.archieved_point is None:
            self.archieved_point = new_point
        else:
            self.snapshot_point = new_point

    def update_snapshot(self, new_point: DataPoint) -> None:
        self.snapshot_point = new_point

    def save_snapshot(self, new_point: DataPoint) -> DataPoint:
        save_point = self.snapshot_point
        self.archieved_point = self.snapshot_point
        self.snapshot_point = new_point
        return save_point
