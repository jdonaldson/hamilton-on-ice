#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import os

from hamilton_ice.util.dag import topo_sort, GraphError

__author__ = "Justin Donaldson"
__copyright__ = "Justin Donaldson"
__license__ = "mit"

def test_simple_toposort():
    graph = {
            "foo" : ["bar"],
            "bar" : [],
            "bing" :["foo"]
    }

    def get_parent(node):
        return graph[node]

    results = topo_sort(graph.keys(), get_parent)

    assert ",".join(results) == "bar,foo,bing"

def test_toposort_fail():
    graph = {
            "foo" : ["bar"],
            "bar" : ["foo"],
    }

    def get_parent(node):
        return graph[node]

    try:
        results = topo_sort(graph.keys(), get_parent)
        assert False, "Test should throw an error"
    except GraphError as err:
        assert True




