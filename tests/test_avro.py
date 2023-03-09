# -*- coding: utf-8 -*-

import os

from hamilton_ice.io.artifact import artifact
from hamilton_ice.io.avro import avro_source, avro
from hamilton_ice.pipeline import build_pipeline

from fastavro import writer, parse_schema

local_params = {
    'source': os.getcwd() + "/tests/data/source.avro",
    'hamilton_ice_cache_dir': os.getcwd() + "/tests/data/"
}

def write_test_data():
    records = [{"foo" : "bar", "baz" : 9}, {"foo" : "bar2", "baz" : 92}]
    schema = {
        'doc': 'test data',
        'name': 'test',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {'name': 'foo', 'type': 'string'},
            {'name': 'baz', 'type': 'int'},
        ],
    }

    parsed_schema = parse_schema(schema)

    with open(os.getcwd() + "/tests/data/source.avro", 'wb') as fh:
        writer(fh, parsed_schema, records)

# use this to regenerate test avro data
# write_test_data()

class ExampleAvroClass:
    @artifact
    def params():
        return local_params

    @avro_source
    def source(params):
        return params['source']

    @avro
    def process(source):
        def mapf(s):
            s['foo'] += '3'
            s['baz'] *= 10
            return s

        return list(map(mapf, source))

build_pipeline(ExampleAvroClass)

def test_source_equality():
    generator = ExampleAvroClass.source.generator
    itr = generator(False, False)
    block = next(itr)
    block_iter = iter(block)
    record1 = next(block_iter)
    assert record1['foo'] == "bar"
    assert record1['baz'] == 9
    record2 = next(block_iter)
    assert record2['foo'] == "bar2"
    assert record2['baz'] == 92

def test_generator_equality():
    generator = ExampleAvroClass.process.generator
    records = [r for r in generator(False, False)]

    assert records[0][0]['foo'] == "bar3"
    assert records[0][0]['baz'] == 90

    assert records[0][1]['foo'] == "bar23"
    assert records[0][1]['baz'] == 920
