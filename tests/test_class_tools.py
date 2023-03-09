#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Testing the class tools for extended attribute detection on class fields.

E.g., instance field attribute checks not only check the base instance, but also
any extended classes.
"""

from hamilton_ice.util.clazz import  has_field_attr, get_field_attr

__author__ = "Justin Donaldson"
__copyright__ = "Justin Donaldson"
__license__ = "mit"


class BaseClass:
    def __init___(self):
        pass
    @staticmethod
    def foo():
        pass

    @staticmethod
    def bar():
        pass

BaseClass.bar.attrib = 'set_value'

class ExtendedClass(BaseClass):
    def __init___(self):
        pass
    @staticmethod
    def foo(self):
        pass


class ExtendedModifiedClass(BaseClass):
    def __init___(self):
        pass

    @staticmethod
    def bar():
        pass

    @staticmethod
    def foo():
        pass

ExtendedModifiedClass.foo.attrib = 'set_value'


def test_get_field_attr():
    assert get_field_attr(ExtendedModifiedClass, 'bar', 'attrib') == 'set_value'
    assert get_field_attr(ExtendedModifiedClass, 'foo', 'attrib') == 'set_value'
    try:
        get_field_attr(ExtendedClass, 'foo', 'attrib') == 'set_value'
        assert False, "Should throw an error"
    except ValueError:
        assert True



def test_has_field_attr():
    assert has_field_attr(ExtendedModifiedClass, 'bar', 'attrib')
    assert has_field_attr(ExtendedModifiedClass, 'foo', 'attrib')

    assert has_field_attr(BaseClass, 'bar', 'attrib')
    assert not has_field_attr(BaseClass, 'foo', 'attrib')

    assert has_field_attr(ExtendedClass, 'bar', 'attrib')
    assert not has_field_attr(ExtendedClass, 'foo', 'attrib')

