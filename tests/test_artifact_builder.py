# -*- coding: utf-8 -*-

from hamilton_ice.generator import build_artifact_generator
from hamilton_ice.io.artifact import artifact, ArtifactIO

from hamilton_ice.pipeline import build_pipeline, reset_artifacts


class ExampleArtifactClass:
    @artifact
    def params():
        return {'config': 'value_from_params'}

    @artifact
    def config(params):
        return params['config']


build_pipeline(ExampleArtifactClass)


def test_artifact_created():
    artf = ExampleArtifactClass.params.artifact()
    config = artf['config']
    assert config == "value_from_params"


def test_artifact_multiple_reset():
    try:
        reset_artifacts(ExampleArtifactClass)
        reset_artifacts(ExampleArtifactClass)
    except ValueError as e:
        assert False
    assert True

def test_artifact_reset():
    config = ExampleArtifactClass.config.artifact()
    assert config == "value_from_params"
    assert hasattr(ExampleArtifactClass.config, "_artifact")
    reset_artifacts(ExampleArtifactClass)
    assert not hasattr(ExampleArtifactClass.config, "_artifact")
