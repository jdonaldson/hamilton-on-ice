from abc import abstractmethod, ABCMeta
from typing import Dict, Generic, TypeVar

import smart_open

from hamilton_ice.util.io import output_name

Type = TypeVar('T')


class BaseLoader(Generic[Type], metaclass=ABCMeta):
    """ The base inheritance class for any hamilton ice loader """
    def __init__(self, location, batch_size=32):
        self.location = location
        if batch_size < 0:
            raise "Batch size must be greater than zero"
        self.batch_size = batch_size

    @abstractmethod
    def load(self) -> Type:
        """ Should load a single value """

    def exists(self) -> bool:
        """ Should return true if dumped results exist """
        with smart_open.open(self.location, mode='rb') as handle:
            byte = handle.read(1)
        return byte != ""


class BaseDumper(metaclass=ABCMeta):
    """ The base inheritance class for any hamilton ice dumper """
    @abstractmethod
    def dump(self, value) -> None:
        """ Should dump a single value """

    @abstractmethod
    def close(self) -> None:
        """ Should save results and close """


class BaseIO(metaclass=ABCMeta):
    """ The base IO class for any hamilton ice io decorator """
    def __init__(self, cls, fn_name):
        self.cls = cls
        self.fn_name = fn_name
        self.fn = getattr(cls, fn_name)

    @abstractmethod
    def loader(self) -> BaseLoader:
        """ Should create a loader """

    @abstractmethod
    def dumper(self) -> BaseDumper:
        """ Should create a dumper """

    def params(self) -> Dict:
        """ Should return the class params """
        return getattr(self.cls, "params")()

    def batch_size(self) -> int:
        params = self.params()
        if "batch_size" in params:
            return params["batch_size"]
        else:
            return 32


    def location(self):
        """ Should return the io location for reads/writes """
        return output_name(self.fn_name, self.extension(), self.params())


def source(fn):
    fn.is_source = True
    return fn

