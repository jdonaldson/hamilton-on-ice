"""
The Beacon class is a very limited version of a hamilton_ice IO method.
Beacons invoke their functions once, and then yield the value indefinitely.
"""
from hamilton_ice.io.dummy import DummyDumper
from hamilton_ice.io.base import BaseLoader, BaseIO


class BeaconLoader(BaseLoader):
    def __init__(self, func):
        self.func = func

    def load(self):
        yield from self.func.generator(True, False)

    def exists(self):
        return True


class BeaconIO(BaseIO):
    def loader(self):
        return BeaconLoader(self.fn)

    def dumper(self):
        return DummyDumper()


def beacon(func):
    func.io = BeaconIO 
    func.is_beacon = True
    func = staticmethod(func)
    return func
