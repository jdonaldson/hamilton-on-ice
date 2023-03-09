# -*- coding: utf-8 -*-

import os

from hamilton_ice.generator import build_source_generator, build_generator
from hamilton_ice.io.artifact import artifact
from hamilton_ice.io.jsonl import jsonl_source, jsonl


local_params = {
    'source': os.getcwd() + "/tests/data/source.jsonl",
    'hamilton_ice_cache_dir': os.getcwd() + "/tests/data/",
    'batch_size': 1
}


class ExampleJsonlClass:
    @artifact
    def params():
        return local_params

    @jsonl_source
    def source(params):
        return params['source']

    @jsonl
    def process(source):
        def mapf(s):
            s['foo'] += '3'
            s['baz'] *= 10
            return s

        return list(map(mapf, source))


# Manually build the IO generator functions
# The pipeline class will do this automatically,
# but that will be tested separately


# build params
ExampleJsonlClass.params.generator = \
    build_generator(ExampleJsonlClass, 'params')

# build the io class for source
source_io = ExampleJsonlClass.source.io(ExampleJsonlClass, 'source')

# build location generator for source
ExampleJsonlClass.source.location_generator = \
    build_generator(ExampleJsonlClass, 'source')

# build record generator for source
ExampleJsonlClass.source.generator = \
    build_source_generator(ExampleJsonlClass, 'source')


# link the loader and dumper
ExampleJsonlClass.source.loader = source_io.loader
ExampleJsonlClass.source.dumper = source_io.dumper

# build the io class for process
process_io = ExampleJsonlClass.process.io(ExampleJsonlClass, 'process')

# build process generator
ExampleJsonlClass.process.generator = \
    build_generator(ExampleJsonlClass, 'process')


ExampleJsonlClass.process.loader = process_io.loader
ExampleJsonlClass.process.dumper = process_io.dumper


def test_source_equality():
    generator = ExampleJsonlClass.source.generator

    itr = generator(False, False)
    record1 = next(itr)
    assert record1[0]['foo'] == "bar"
    assert record1[0]['baz'] == 9
    record2 = next(itr)
    assert record2[0]['foo'] == "bar2"
    assert record2[0]['baz'] == 92


def test_generator_equality():
    generator = ExampleJsonlClass.process.generator

    records = [r for r in generator(False, False)]
    assert(len(records) == 2)
    assert records[0][0]['foo'] == "bar3"
    assert records[0][0]['baz'] == 90

    assert records[1][0]['foo'] == "bar23"
    assert records[1][0]['baz'] == 920

