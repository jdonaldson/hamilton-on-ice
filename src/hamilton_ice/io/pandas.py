import smart_open
import pandas as pd

from hamilton_ice.io.dummy import DummyDumper
from hamilton_ice.io.base import BaseLoader, BaseIO, BaseDumper
from hamilton_ice.io.base import source



class PandasCsvLoader(BaseLoader):
    def load(self):
        yield from pd.read_csv(self.location, chunksize=self.batch_size, engine="python")


class PandasCsvSourceIO(BaseIO):
    def loader(self):
        location = next(self.fn.location_generator())
        return PandasCsvLoader(location, self.batch_size())

    def dumper(self):
        return DummyDumper()

    def extension(self):
        return 'csv'


def pandas_csv_source(fn):
    fn = source(fn)
    fn.io = PandasCsvSourceIO
    return fn


class PandasMsgPackLoader(BaseLoader):
    def load(self):
        with smart_open.open(self.location, 'rb') as handle:
            yield from pd.read_msgpack(handle, iterator=True)


class PandasMsgPackDumper(BaseDumper):
    def __init__(self, location):
        self.location = location
        self.append = False

    def dump(self, rec):
        rec.to_msgpack(self.location, append=self.append)
        if not self.append:
            self.append = True

    def close(self):
        pass


class PandasMsgPackIO(BaseIO):
    def loader(self):
        return PandasMsgPackLoader(self.location())

    def dumper(self):
        return PandasMsgPackDumper(self.location())

    def extension(self):
        return 'msg'


def pandas_msgpack(fn):
    fn.io = PandasMsgPackIO
    return fn
