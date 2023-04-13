# -*- coding: utf-8 -*-

from hamilton_ice.pipeline import get_func_args, object_io_nodes, build_pipeline
from hamilton_ice.io.beacon import beacon
from hamilton_ice.io.jsonl import jsonl_source, jsonl


class Example:
    @beacon
    def foo(bar, baz):
        return {"bar": bar["bar"], "baz": baz["baz"]}

    @beacon
    def bar():
        return {"bar": 1}

    @beacon
    def baz():
        return {"baz": 2}

    @staticmethod
    def bing():
        pass

    @jsonl_source
    def dummy_source():
        pass

    @jsonl
    def dummy_io():
        pass


def test_get_func_args():

    result = get_func_args(Example, 'foo')
    assert(",".join(result) == "bar,baz")

    result = get_func_args(Example, 'iter_count')
    assert(",".join(result) == "")

    result = get_func_args(Example, 'epoch_count')
    assert(",".join(result) == "")


def test_object_io_nodes():
    result = object_io_nodes(Example)
    result.sort()
    assert(",".join(result) == "bar,baz,dummy_io,dummy_source,foo")


def test_build_pipeline():
    build_pipeline(Example)
    assert(hasattr(Example.foo, "generator"))
    assert(hasattr(Example.bar, "generator"))
    assert(hasattr(Example.baz, "generator"))
    assert(hasattr(Example.dummy_source, "generator"))
    assert(not hasattr(Example.bing, "generator"))


def test_simple_beacon_pipeline():
    result = next(Example.foo.generator())
    assert(result["bar"] == 1)
    assert(result["baz"] == 2)
