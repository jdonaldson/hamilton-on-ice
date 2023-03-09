"""
The Artifact class is a very limited version of a hamilton_ice IO method.
Artifacts invoke their functions once, and then yield the value indefinitely.
"""
from hamilton_ice.io.dummy import DummyDumper
from hamilton_ice.io.base import BaseLoader, BaseIO


class ArtifactLoader(BaseLoader):
    def __init__(self, func):
        self.func = func

    def load(self):
        yield from self.func.generator(True, False)

    def exists(self):
        return True


class ArtifactIO(BaseIO):
    def loader(self):
        return ArtifactLoader(self.fn)

    def dumper(self):
        return DummyDumper()


def artifact(func):
    func.io = ArtifactIO
    func.is_artifact = True
    func = staticmethod(func)
    return func
