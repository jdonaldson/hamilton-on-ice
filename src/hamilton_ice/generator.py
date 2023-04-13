from types import GeneratorType
from typing import Callable, TypeVar, Any, Dict
import inspect


Type = TypeVar('Type')

"""

The following is the type definition for the main "generator" function
constructed for each IO method.

The generator function accepts three arguments :

    use_loaders: bool        Indicates that the generator should use the loader
                             (usually a deserializer) for any upstream IO
                             method.

    save_output : bool       Indicates that the generator should save the
                             output of the current IO method.

    proxy : Dict[String,Any] If the proxy dictionary is passed and contains a
                             key that matches an upstream IO method name, the
                             corresponding dictionary value will be used
                             as a one-off generated value instead.
"""
GeneratorT = Callable[[bool, bool, Dict[str, Any]], Type]

def build_beacon_accessor(obj: object, fn_name: str):
    """
    builds a simple accessor for beacons for the given object and fn_name.
    The beacon accessor enables the retrieval of a beacon without having
    to iterate through a generator
    """

    func = getattr(obj, fn_name)
    def beacon_accessor():
        if not hasattr(func, "_beacon"):
            func._beacon = next(func.generator())
        return func._beacon
    return beacon_accessor

def build_beacon_generator(obj: object, fn_name: str) \
        -> GeneratorT:
    """
    build a simple generator for beacons given an object and a function name
    on the given object.  The generator function accepts two parameters related
    to saving/loading output.  However, for beacons these are ignored.
    """
    func = getattr(obj, fn_name)

    varnames = inspect.getfullargspec(func).args
    value_generators = [getattr(obj, v).generator() for v in varnames]
    zip_args = zip(*value_generators)


    def create_beacon():
        if not varnames:
            return func()
        args = next(zip_args)
        kwargs = dict(zip(varnames, args))
        return func(**kwargs)


    def beacon_generator(_=None, __=None, ___=None):
        """See GeneratorT docs"""

        if not hasattr(func, '_beacon'):
            func._beacon = create_beacon()

        if isinstance(func._beacon, GeneratorType):
          raise ValueError("The beacon '%s.%s' should not be a generator" % (obj.__name__, fn_name) )
        else:
          while True:
              yield func._beacon

    return beacon_generator


def build_source_generator(obj: object, fn_name: str) -> GeneratorT:
    """
    Build a simple generator for source streams given an object and a function
    name on the given object.  The generator function accepts two parameters
    related to saving/loading output.  However, for sources, both are ignored.
    """
    func = getattr(obj, fn_name)

    def source_generator(_=None, __=None, ___=None):
        """See GeneratorT docs"""
        loader = func.loader().load()
        yield from loader

    return source_generator


def once(value):
    yield value

def check_valid(obj, fn_name, gen_name, ancestors):
    # if fn_name in ancestors:
    #     raise ValueError("%s is used multiple times in the dependencies for %s (check the arguments of your function, and the arguments of those function arguments)" % (fn_name, gen_name))
    func = getattr(obj, fn_name)
    varnames = inspect.getfullargspec(func).args
    for v in varnames:
        check_valid(obj, v, fn_name, ancestors)
    if "is_beacon" not in func.__dict__:
        ancestors.append(fn_name)
    return True

def build_generator(obj: object, fn_name: str) -> GeneratorT:
    func = getattr(obj, fn_name)
    epoch_count = 0
    varnames = inspect.getfullargspec(func).args
    check_valid(obj, fn_name, fn_name, [])

    def generator(use_loaders=False, save_output=False, proxy=None):
        """See GeneratorT docs"""
        iter_count = 0

        if save_output:
            dumper = func.dumper()


        def iter_count_gen():
            while True:
                yield iter_count

        def epoch_count_gen():
            while True:
                yield epoch_count

        def gen_fn_name(varname, proxy):

            if varname == "iter_count":
                return iter_count_gen()
            elif varname == "epoch_count":
                return epoch_count_gen()
            elif proxy is not None and varname in proxy:
                return once(proxy[varname])
            elif use_loaders:
                return getattr(obj, varname).loader().load()
            else:
                return getattr(obj, varname) \
                    .generator(use_loaders, save_output, proxy)

        value_generators = [gen_fn_name(v, proxy) for v in varnames]


        zip_args = zip(*value_generators)

        def is_infinite(varname):
            # generators for beacons repeat return values over and over
            # in this case, _beacon will be a static value,
            # instead of a generator
            attr = getattr(obj, varname)
            if hasattr(attr, "_beacon"):
              return not isinstance(getattr(attr, "_beacon"), GeneratorType)

        if not varnames:
            raise ValueError('''
                Empty arg functions like %s() should be beacons or sources
                ''' % fn_name)

        for args in zip_args:
            if any(map(lambda x: x is None, args)):
              continue

            kwargs = dict(zip(varnames, args))
            result = func(**kwargs)

            if result is None:
                iter_count += 1
                continue
            elif isinstance(result, GeneratorType):
                for record in result:
                    iter_count += 1
                    if record is not None:
                      if save_output:
                          dumper.dump(record)
                      yield record
            else:
                if save_output:
                    dumper.dump(result)
                iter_count += 1
                yield result

            # if all varnames are infinite generators, break
            if all(map(lambda x: is_infinite(x), varnames)):
                break

        if save_output:
            dumper.close()

        nonlocal epoch_count
        epoch_count += 1

    return generator

