# -*- coding: utf-8 -*-

from hamilton_ice.generator import build_beacon_generator
from hamilton_ice.io.beacon import beacon, BeaconIO

from hamilton_ice.pipeline import build_pipeline, reset_beacons


class ExampleBeaconClass:
    @beacon
    def params():
        return {'config': 'value_from_params'}

    @beacon
    def config(params):
        return params['config']


build_pipeline(ExampleBeaconClass)


def test_beacon_created():
    artf = ExampleBeaconClass.params.beacon()
    config = artf['config']
    assert config == "value_from_params"


def test_beacon_multiple_reset():
    try:
        reset_beacons(ExampleBeaconClass)
        reset_beacons(ExampleBeaconClass)
    except ValueError as e:
        assert False
    assert True

def test_beacon_reset():
    config = ExampleBeaconClass.config.beacon()
    assert config == "value_from_params"
    assert hasattr(ExampleBeaconClass.config, "_beacon")
    reset_beacons(ExampleBeaconClass)
    assert not hasattr(ExampleBeaconClass.config, "_beacon")
