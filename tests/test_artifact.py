# -*- coding: utf-8 -*-

from hamilton_ice.generator import build_beacon_generator, build_beacon_accessor
from hamilton_ice.io.beacon import beacon, BeaconIO


class ExampleBeaconClass:
    @beacon
    def params():
        return {'config': 'value_from_params'}

    @beacon
    def config(params):
        return params['config']

    @beacon
    def illegal_generator():
        while True:
            yield 0

# Manually build the IO generator functions
# The pipeline class will do this automatically,
# but that will be tested separately

ExampleBeaconClass.params.generator = \
    build_beacon_generator(ExampleBeaconClass, 'params')


ExampleBeaconClass.params.beacon = \
    build_beacon_accessor(ExampleBeaconClass, 'params')

ExampleBeaconClass.config.generator = \
    build_beacon_generator(ExampleBeaconClass, 'config')



def test_beacon_generator_equality():
    generator = build_beacon_generator(ExampleBeaconClass, 'params')
    beacon1 = next(generator(False, False))
    beacon2 = next(generator(False, False))

    assert beacon1 == beacon2


def test_beacon_attributes():
    assert hasattr(ExampleBeaconClass.params, 'is_beacon')
    assert getattr(ExampleBeaconClass.params, 'is_beacon')
    assert hasattr(ExampleBeaconClass.params, 'beacon')
    assert hasattr(ExampleBeaconClass.params, 'io')


def test_beacon_generator_illegal_yield():
    generator = build_beacon_generator(ExampleBeaconClass, 'illegal_generator')
    try:
        next(generator(False, False))
    except ValueError:
        assert True


def test_beacon_io():
    aio = beaconIO(ExampleBeaconClass, 'params')
    loader = aio.loader()
    assert loader.exists()
    load_itr = loader.load()
    beacon1 = next(load_itr)
    beacon2 = next(load_itr)
    assert beacon1 == beacon2
    dumper = aio.dumper()
    dumper.dump(None)


def test_beacon_dependency_io():
    aio = beaconIO(ExampleBeaconClass, 'config')
    loader = aio.loader()
    assert loader.exists()
    beacon1 = next(loader.load())
    assert beacon1 == "value_from_params"
