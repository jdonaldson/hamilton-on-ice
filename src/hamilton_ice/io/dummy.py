"""
The Dummy class/types satisfy the API of the BaseIO/Dumper/Loader class, but
they ignore all operations
"""

from hamilton_ice.io.base import BaseDumper, BaseLoader, BaseIO


class DummyIO(BaseIO):
    def loader(self):
        return DummyLoader()

    def dumper(self):
        return DummyDumper()


class DummyLoader(BaseLoader):
    def __init__(self):
        pass

    def load(self):
        pass

class DummyDumper(BaseDumper):
    def __init__(self):
        pass

    def dump(self, value):
        pass

    def close(self):
        pass

class DummySourceLoader(BaseLoader):
  def load(self):
    generator = self.location()
    for rec in generator:
      yield rec

class DummySourceIO(DummyIO):
  def loader(self):
    return DummySourceLoader(next(self.fn.location_generator()))


def dummy(fn):
    fn.io = DummyIO
    fn = staticmethod(fn)
    return fn

def dummy_source(fn):
    fn.io = DummySourceIO
    fn.is_source = True
    fn = staticmethod(fn)
    return fn
