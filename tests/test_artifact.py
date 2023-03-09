# -*- coding: utf-8 -*-

from hamilton_ice.generator import build_artifact_generator, build_artifact_accessor
from hamilton_ice.io.artifact import artifact, ArtifactIO


class ExampleArtifactClass:
    @artifact
    def params():
        return {'config': 'value_from_params'}

    @artifact
    def config(params):
        return params['config']

    @artifact
    def illegal_generator():
        while True:
            yield 0

# Manually build the IO generator functions
# The pipeline class will do this automatically,
# but that will be tested separately

ExampleArtifactClass.params.generator = \
    build_artifact_generator(ExampleArtifactClass, 'params')


ExampleArtifactClass.params.artifact = \
    build_artifact_accessor(ExampleArtifactClass, 'params')

ExampleArtifactClass.config.generator = \
    build_artifact_generator(ExampleArtifactClass, 'config')



def test_artifact_generator_equality():
    generator = build_artifact_generator(ExampleArtifactClass, 'params')
    artifact1 = next(generator(False, False))
    artifact2 = next(generator(False, False))

    assert artifact1 == artifact2


def test_artifact_attributes():
    assert hasattr(ExampleArtifactClass.params, 'is_artifact')
    assert getattr(ExampleArtifactClass.params, 'is_artifact')
    assert hasattr(ExampleArtifactClass.params, 'artifact')
    assert hasattr(ExampleArtifactClass.params, 'io')


def test_artifact_generator_illegal_yield():
    generator = build_artifact_generator(ExampleArtifactClass, 'illegal_generator')
    try:
        next(generator(False, False))
    except ValueError:
        assert True


def test_artifact_io():
    aio = ArtifactIO(ExampleArtifactClass, 'params')
    loader = aio.loader()
    assert loader.exists()
    load_itr = loader.load()
    artifact1 = next(load_itr)
    artifact2 = next(load_itr)
    assert artifact1 == artifact2
    dumper = aio.dumper()
    dumper.dump(None)


def test_artifact_dependency_io():
    aio = ArtifactIO(ExampleArtifactClass, 'config')
    loader = aio.loader()
    assert loader.exists()
    artifact1 = next(loader.load())
    assert artifact1 == "value_from_params"
