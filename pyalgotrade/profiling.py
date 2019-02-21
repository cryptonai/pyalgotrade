import tracemalloc
import datetime as dt


_started = False

class Snapshot(object):

    def __init__(self):
        self._snapshot = tracemalloc.take_snapshot()
        self._time = dt.datetime.utcnow()
    
    @property
    def time(self):
        return self._time

    @property
    def snapshot(self):
        return self._snapshot


def take_snapshot():
    if not _started:
        _started = True
        tracemalloc.start()
    return Snapshot()

def compare_snapshots(snapshot2, snapshot1, cmp_type='lineno'):
    assert isinstance(snapshot1, Snapshot) and isinstance(snapshot2, Snapshot)
    top_stats = snapshot2.snapshot.compare_to(snapshot1.snapshot, cmp_type)
    return top_stats
