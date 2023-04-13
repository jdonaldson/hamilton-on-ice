import inspect

from .util.clazz import get_field_attr, has_field_attr
from .util.dag import topo_sort
from .generator import build_generator, build_source_generator, \
        build_beacon_generator, build_beacon_accessor


def get_func_args(obj, field):
    """
    Given an object and a function (field name) definition, returns the valid
    argument names for the function.  This process removes 'reserved' argument
    names that correspond to hard-wired generator behavior.
    """
    if field in ["iter_count", "epoch_count"]:
        return []
    else:
        args = inspect.getfullargspec(getattr(obj, field)).args
        args = [a for a in args if a not in ["iter_count", "epoch_count"]]
        return args


def object_io_nodes(obj):
    """
    Given an object/class, returns a list of the method names that have valid
    IO decorators applied
    """
    def valid_node(field):
        return has_field_attr(obj, field, 'io') and \
                callable(getattr(obj, field))

    io_nodes = [m for m in dir(obj) if valid_node(m)]

    return topo_sort(io_nodes, lambda node: get_func_args(obj, node))


def reset_beacons(obj):
    """
    Resets all beacons in the given pipeline (forcing them to be recreated)
    """
    modified = False
    for node in object_io_nodes(obj):
        fn = getattr(obj, node)
        if has_field_attr(obj, node, 'is_beacon') \
                and has_field_attr(obj, node, "_beacon"):
                    modified = True
                    del fn._beacon
    if modified:
        build_pipeline(obj)


def build_pipeline(obj):
    """
    Build all necessary IO fields on a pipeline object.
    This process involves iterating through the object fields, finding the
    methods with IO decorators, determining the type of generator function to
    apply (e.g. source/beacon/default), and building the function for the
    field's 'generator' attribute.
    """
    io_nodes = object_io_nodes(obj)

    for node in io_nodes:
        fn = getattr(obj, node)

        io = get_field_attr(obj, node, 'io')(obj, node)

        fn.loader = io.loader
        fn.dumper = io.dumper

    for node in io_nodes:
        fn = getattr(obj, node)
        if has_field_attr(obj, node, 'is_source'):
            fn.location_generator = build_generator(obj, node)
            fn.generator = build_source_generator(obj, node)
        elif has_field_attr(obj, node, 'is_beacon'):
            fn.generator = build_beacon_generator(obj, node)
            fn.beacon = build_beacon_accessor(obj, node)
        else:
            fn.generator = build_generator(obj, node)
