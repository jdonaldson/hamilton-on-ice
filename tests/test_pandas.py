# -*- coding: utf-8 -*-

import os

from hamilton_ice.io.artifact import artifact
from hamilton_ice.io.pandas import pandas_csv_source, pandas_msgpack
from hamilton_ice.pipeline import build_pipeline

from csv import DictWriter

local_params = {
    'source': os.getcwd() + "/tests/data/source.csv",
    'hamilton_ice_cache_dir': os.getcwd() + "/tests/data/",
    'batch_size': 1
}

def write_test_data():
    records = [{"foo" : "bar", "baz" : 9}, {"foo" : "bar2", "baz" : 92}]


    with open(os.getcwd() + "/tests/data/source.csv", 'w') as fh:
        writer = DictWriter(fh,fieldnames=["foo", "baz"])
        writer.writeheader()
        writer.writerows(records)

# use this to regenerate test csv data
# write_test_data()

class ExampleCsvClass:
    @artifact
    def params():
        return local_params

    @pandas_csv_source
    def source(params):
        return params['source']

    @pandas_msgpack
    def process(source):
        source['foo'] += '3'
        source['baz'] *= 10
        return source

build_pipeline(ExampleCsvClass)

def test_source_equality():
    generator = ExampleCsvClass.source.generator
    itr = generator(False, False)

    record1 = next(itr)
    assert len(record1) == 1
    assert record1['foo'][0] == "bar"
    assert record1['baz'][0] == 9

    record2 = next(itr)
    assert len(record2) == 1
    assert record2['foo'][1] == "bar2"
    assert record2['baz'][1] == 92

def test_generator_equality():
    generator = ExampleCsvClass.process.generator
    records = [r for r in generator(False, False)]
    assert len(records) == 2
    record1 = records[0]
    record2 = records[1]

    assert record1['foo'][0] == "bar3"
    assert record1['baz'][0] == 90

    assert record2['foo'][1] == "bar23"
    assert record2['baz'][1] == 920
