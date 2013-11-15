"""
Function for drawing nodes in a context as a .dot file.

This is outside context.py as it doesn't need to by
cythoned and it's convenient to use inner functions,
which isn't supported in cython. 
"""
from nodes import MDFNode, MDFVarNode,_now_node
from nodetypes import  MDFQueueNode
import pydot
import os

def _to_dot(ctx,
            filename=None,
            nodes=None,
            colors={},
            all_contexts=True,
            max_depth=None,
            rankdir="LR"):
    """
    constructs a .dot graph from the nodes that have a value in
    this context and writes it to filename if not None.

    colors can be used to override any of the colors used
    to color the graph. the defaults are::

        defaults = {
            "node"      :   "white",
            "nownode"   :   "darkorchid1",
            "queuenode" :   "darksalmon",
            "varnode"   :   "deepskyblue",
            "shiftnode" :   "gold",
            "headnode"  :   "olivedrab3",
            "edge"      :   "black",
            "nowedge"   :   "darkorchid4",
            "varedge"   :   "deepskyblue4",
            "shiftedge" :   "gold4",
            "context"   :   "grey90",
            "module0"   :   "grey81",
            "module1"   :   "grey72"
        }

    If all_contexts is true it will look for head nodes in all contexts,
    otherwise only this context will be used.

    If max_depth is not None the graph will be truncated so that all nodes
    are at most max_depth levels deep from the root node(s).

    rankdir sets how the graph is ordered when rendered.
    Possible values are:

    - "TB" : top to bottom
    - "LR" : left to right
    - "BT" : bottom to top
    - "RL" : right to left

    returns a pydot.Graph object
    """
    root = pydot.Dot("root", graph_type="digraph", simplify=True)
    root.set("rankdir", rankdir)

    # dict of graph_name -> pydot Subgraph
    sub_graphs = {}

    # dict of (ctx, node) -> pydot Node
    graph_nodes = {}

    # list of ((calling node, calling context), (called node, called context), depth)
    dependencies = []

    contexts = [ctx]
    if all_contexts:
        parent = ctx.get_parent() or ctx
        contexts = [parent] + parent.get_shifted_contexts()

    if nodes is None:
        for x in contexts:
            for node in x.all_nodes():
                if node.has_value(x) or node.was_called(x):
                    dependencies.append(((None, None), (node, x), 0))
    else:
        for node in nodes:
            assert isinstance(node, MDFNode), "expected a list of nodes"
            found_ctx = False
            for x in contexts:
                if node.has_value(x) or node.was_called(x):
                    found_ctx = True
                    dependencies.append(((None, None), (node, x), 0))

            # make sure the nodes appear in at least one context, even
            # if it has no value
            if not found_ctx:
                dependencies.append(((None, None), (node, ctx), 0))

    _short_names = {}
    def _get_short_name(long_name, prefix="x"):
        if long_name in _short_names:
            return _short_names[long_name]
        short_name = "%s%d" % (prefix, len(_short_names))
        _short_names[long_name] = short_name
        return short_name

    def _get_graph(ctx, node):
        # build all the subgraphs, starting with the root package
        ctx_label = _escape_node_label(str(ctx))
        subgraphs = [("cluster_%d" % ctx._id, ctx_label, colors["context"])]

        package = (node.modulename or "")
        name = ("cluster_%d_%s" % (ctx._id, _get_short_name(package, "p")))
        label = _escape_node_label(package)
        subgraphs.append((name, label, colors["module0"]))

        parent = sub_graph = root
        for name, label, color in subgraphs:
            sub_graph = sub_graphs.get(name)
            if sub_graph is None:
                sub_graph = pydot.Subgraph(name)
                sub_graph.set("label", label)
                if color is not None:
                    sub_graph.set("style", "filled")
                    sub_graph.set("fillcolor", color)
                sub_graphs[name] = sub_graph
                parent.add_subgraph(sub_graph)
            parent = sub_graph

        return sub_graph

    def _get_node(ctx, node, depth):
        graph_node = graph_nodes.get((ctx, node.name))
        if graph_node is None and (max_depth is None or depth < max_depth):
            long_name = _escape_node_label("%s.%d" % (node.name, ctx._id))
            graph_node = pydot.Node(_get_short_name(long_name, "n"))
            graph_node.set("label", _escape_node_label(node.short_name))

            if node in ctx.get_shift_set():
                graph_node.set("style", "filled")
                graph_node.set("fillcolor", colors["shiftnode"])
            elif node is _now_node:
                graph_node.set("style", "filled")
                graph_node.set("fillcolor", colors["nownode"])
            elif isinstance(node, MDFVarNode):
                graph_node.set("style", "filled")
                graph_node.set("fillcolor", colors["varnode"])
            elif not node.get_callers(ctx):
                graph_node.set("style", "filled")
                graph_node.set("fillcolor", colors["headnode"])
            elif isinstance(node, MDFQueueNode):
                graph_node.set("style", "filled")
                graph_node.set("fillcolor", colors["queuenode"])
            else:
                graph_node.set("style", "filled")
                graph_node.set("fillcolor", colors["node"])

            graph_nodes[(ctx, node.name)] = graph_node
            graph = _get_graph(ctx, node)
            graph.add_node(graph_node)
        return graph_node

    edges = set()
    def _add_edge(a, b, ctx, node):
        if (a, b) not in edges:
            edge = pydot.Edge(a, b)

            if node in ctx.get_shift_set():
                edge.set("color", colors["shiftedge"])
            elif node is _now_node:
                edge.set("color", colors["nowedge"])
            elif isinstance(node, MDFVarNode):
                edge.set("color", colors["varedge"])
            else:
                edge.set("color", colors["edge"])

            edges.add(edge)
            root.add_edge(edge)
            
    def _escape_node_label(label):
        """
        due to a bug in pydot single quotes are not escaped correctly causing pydot to fail. We can have single quotes 
        for class representations in the context for instance.  
        """
        escapes = [
            ("'", r"\'"),
            ('"', r'\"'),
            ('"', r'\"'),
            (':', r'_'),
        ]

        for a, b in escapes:
            label = label.replace(a, b)

        return label

    # nodes deeper than max_depth are not added, but instead of checking in this loop
    # and continuing based on depth > max_depth it's done in _get_node. This is because
    # you could have a node at depth max_depth with an edge linking to a node at depth
    # max_depth-n and other nodes at max_depth+1. In that case the edge needs to be added
    # but the other nodes don't - so _get_node won't create nodes below max_depth, but
    # if the node already exists it will add the edge.
    #
    # do two passes to pick up any missing edges because of truncated nodes (eg if node
    # at max_depth has an edge to another node but that node wasn't added until after
    # the first node had been processed.
    passes = [dependencies]
    if max_depth is not None:
        passes.append(list(dependencies))

    for dependencies in passes:
        seen = set()
        while dependencies:
            (calling_node, calling_ctx), (called_node, called_ctx), depth = dependencies.pop(0)
    
            if (calling_node, calling_ctx, called_node, called_ctx) in seen:
                continue
            seen.add((calling_node, calling_ctx, called_node, called_ctx))
    
            # get the graph nodes
            called_graph_node = _get_node(called_ctx, called_node, depth)
            if called_graph_node is None:
                continue
    
            calling_graph_node = None
            if calling_node is not None and calling_ctx is not None:
                calling_graph_node = _get_node(calling_ctx, calling_node, depth)
    
            # add the edge between the nodes
            if calling_graph_node is not None:
                _add_edge(calling_graph_node, called_graph_node, called_ctx, called_node)
    
            # add any dependencies of the called node
            deps = called_node.get_dependencies(called_ctx)
            for node, ctx in deps:
                dependencies.append(((called_node, called_ctx), (node, ctx), depth + 1))

    # write to the output file if one was specified
    if filename:
        write_method = root.write
        # get the appropriate write method based on the file ext
        if "." in filename:
            unused, ext = os.path.splitext(filename)
            if ext != ".dot":
                ext = ext.strip(".")
                write_method = getattr(root, "write_%s" % ext, write_method)
        write_method(filename)

    return root
