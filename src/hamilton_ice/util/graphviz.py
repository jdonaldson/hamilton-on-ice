from graphviz import Digraph
from hamilton_ice.pipeline import object_io_nodes, get_func_args


def dag_plot(obj):
    io_nodes = object_io_nodes(obj)
    dot = Digraph(comment=obj.__name__)

    for node in io_nodes:
        field = getattr(obj, node)
        if hasattr(field, 'is_artifact'):
            dot.node(node, style="filled", color="lightgrey")
        elif hasattr(field, 'is_source'):
            dot.node(node, style="filled", color="green")
        else:
            dot.node(node, shape="doubleoctagon")

    for node in io_nodes:
        args = get_func_args(obj, node)
        for a in args:
            dot.edge(a, node)

    return dot
