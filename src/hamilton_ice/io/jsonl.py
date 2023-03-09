from itertools import islice

import jsonlines
import smart_open

from hamilton_ice.io.base import BaseIO, BaseLoader, BaseDumper, source
from hamilton_ice.io.dummy import DummyDumper

from hamilton_ice.util.minibatch import batched_generator



class JsonLinesTrainIO(BaseIO):
    def extension(self):
        return "jsonl"

    def loader(self):
        return JsonLinesLoader(self.location(), self.batch_size())

    def dumper(self):
        return DummyDumper()


class JsonLinesIO(BaseIO):

    def loader(self):
        return JsonLinesLoader(self.location(), self.batch_size())

    def dumper(self):
        return JsonLinesDumper(self.location())

    def extension(self):
        return "jsonl"


class JsonLinesLoader(BaseLoader):

    def load(self):
        with smart_open.open(self.location, 'r', encoding="utf8") as handle:
            with jsonlines.Reader(handle) as reader:
                yield from batched_generator(reader, self.batch_size)


class JsonLinesDumper(BaseDumper):
    def __init__(self, location):
        self.location = location
        handle = smart_open.open(location, 'w', encoding="utf8")
        self.writer = jsonlines.Writer(handle)

    def dump(self, rec):
        self.writer.write(rec)

    def close(self):
        self.writer.close()


def jsonl(fn):
    fn.io = JsonLinesIO
    return fn


def jsonl_train(fn):
    fn.io = JsonLinesTrainIO
    return fn


def jsonl_source(fn):
    fn = source(fn)
    fn.io = JsonLinesIO
    return fn
