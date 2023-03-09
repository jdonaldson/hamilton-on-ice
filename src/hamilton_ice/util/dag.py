from typing import TypeVar, Iterable, Callable, Any

T = TypeVar('T')

def topo_sort(graph : Iterable[T], parents : Callable[[T], Iterable[T]]) ->Iterable[T]:
    """ Performs a topological sort of the graph edges defined by graph argument """
    result = []
    marked = set()
    def mark(v, top):
        if id(v) in marked:
            return
        for parent in parents(v):
            if parent is top:
                raise GraphError('Cyclical Graph', parent, graph)
            mark(parent, v)
        marked.add(id(v))
        result.append(v)
    for v in graph:
        mark(v, v)
    return result

class GraphError(ValueError):
    pass


