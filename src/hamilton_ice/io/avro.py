import smart_open
import fastavro
from hamilton_ice.io.base import BaseIO, BaseLoader, BaseDumper, source
from hamilton_ice.io.dummy import DummyDumper


class AvroSourceIO(BaseIO):
    def loader(self):
        return AvroLoader(next(self.fn.location_generator()), self.batch_size())

    def dumper(self):
        return DummyDumper()


class AvroIO(BaseIO):

    def loader(self):
        location = self.fn.output
        return AvroLoader(location)

    def dumper(self):
        location = self.fn.output
        return AvroDumper(location)


class AvroLoader(BaseLoader):
    def load(self):
        handle = smart_open.open(self.location, mode='rb')
        for rec in fastavro.block_reader(handle):
            yield rec


class AvroDumper(BaseDumper):
    def __init__(self, location):
        self.handle = smart_open.open(location, mode='wb')
        self.writer = fastavro.write(self.handle)

    def dump(self, rec):
        self.writer.write(rec)

    def close(self):
        self.writer.close()


def avro_source(fn):
    fn = source(fn)
    fn.io = AvroSourceIO
    fn = staticmethod(fn)
    return fn

def avro(fn):
    fn.io = AvroIO
    fn = staticmethod(fn)
    return fn
