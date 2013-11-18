"""
Functions to provide pickle support to MDF classes
"""
from nodes import MDFNode, MDFVarNode, NodeState
from context import MDFContext, ShiftSet
import cython
import sys
import logging
import os
import struct

import sys
if sys.version_info[0] > 2:
    from builtins import __import__
else:
    from __builtin__ import __import__

_log = logging.getLogger(__name__)

# this is cimported in the .pxd file
# uncomment if not compiling with Cython
#from context import _all_nodes

# for backwards compatibility some code looks at this variable
# to pickle in a way that's compatible with older versions
_pickle_version = cython.declare(int)
_pickle_version = int(os.environ.get("MDF_PICKLE_VERSION", 0))

#
# the standard pickle.Unpickler doesn't handle NaNs
# so we override the load_float method here
#
if sys.version_info[0] <= 2:
    import pickle

    try:
        import numpy as np
        nan = np.nan
        inf = np.inf
    except ImportError:
        nan = float("nan")
        inf = float("inf")

    _special_floats = cython.declare(dict, {
        # NaN
        "nan"       : nan,
        "1.#QNAN"   : nan,
        "-1.#QNAN"  : nan,
        # Quiet NaN
        "1.#IND"    : nan,
        "-1.#IND"   : nan,
        # INF
        "1.#INF"    : inf,
        "-1.#INF"   : -inf,
    })

    _special_binfloats = cython.declare(dict, {
        struct.pack(">d", nan)   : nan,
        struct.pack(">d", inf)   : inf,
        struct.pack(">d", -inf)  : -inf,
    })

    def load_float(unpickler):
        value = unpickler.readline()[:-1]
        special_value = _special_floats.get(value, None)
        if special_value is not None:
            unpickler.append(special_value)
            return
        unpickler.append(float(value))

    def load_binfloat(unpickler, unpack=struct.unpack):
        buf = unpickler.read(8)
        special_value = _special_binfloats.get(buf, None)
        if special_value is not None:
            unpickler.append(special_value)
            return
        unpickler.append(unpack('>d', buf)[0])  

    pickle.Unpickler.dispatch[pickle.FLOAT] = load_float
    pickle.Unpickler.dispatch[pickle.BINFLOAT] = load_binfloat

class MissingNodeError(Exception):
    pass

class NodeStateWrapper(object):
    """
    Wrapper around NodeState to store some extra information
    for use while unpickling to allow the full node_state
    to be re-constructed from pickable data.
    """
    def __init__(self, node_state):
        self.node_state = node_state

        # additional attributes
        self.alt_context_id = None
        self.prev_alt_context_id = None

    def __reduce__(self):
        return (
            _unpickle_node_state,
            _pickle_node_state(self),
            None,
            None,
            None
        )

def _pickle_context(ctx):
    """
    returns a picklable tuple of args to be passed to _unpickle_context
    
    The context and all of its shifted contexts are pickled, along with
    and node states that exist for any of those contexts.
    """
    shifted_ctx = cython.declare(MDFContext)
    node = cython.declare(MDFNode)
    node_state = cython.declare(NodeState)

    all_ctx_ids = [ctx.get_id()]

    # get the shift sets for all shifted contexts
    shift_sets = []
    for shifted_ctx in ctx.get_shifted_contexts():
        shift_set = shifted_ctx.get_shift_set()
        shift_sets.append((shifted_ctx.get_id(), shift_set))
        all_ctx_ids.append(shifted_ctx.get_id())

    # get the cached values for all nodes in any of the contexts we're interested in
    node_states = []
    for node in _all_nodes.itervalues():
        for ctx_id, node_state in node._states.iteritems():
            if ctx_id in all_ctx_ids:
                node_states.append((ctx_id, node, NodeStateWrapper(node_state)))

    return (ctx.__class__,
            ctx.get_id(),
            ctx.get_date(),
            node_states,
            shift_sets)

def _get_node(name, is_bound):
    """returns a node instance from its name"""
    # most nodes will be in _all_nodes already
    node = _all_nodes.get((name, is_bound), None)
    if node is not None:
        return node

    # bound nodes are only registered in _all_nodes once
    # they're accessed from the class. If we've got this far the only
    # types of nodes that we are looking for are bound class nodes.
    if not is_bound or "." not in name:
        raise MissingNodeError("Node not found: '%s'" % name)

    # the module that this node is defined in should already have been imported
    components = name.split(".")
    modulename = components[0]
    module = sys.modules.get(modulename, None)
    while modulename in sys.modules and len(components) > 1:
        module = sys.modules[modulename]
        components.pop(0)
        modulename = ".".join((modulename, components[0]))

    if not components or not module:
        raise MissingNodeError("Node not found: '%s'" % name)

    # get the class and then the node from the module
    obj = module
    while components:
        attr = components.pop(0)
        if ":" in attr:
            # get the class and base classes and get the node from the base class,
            # but bound to the parent class
            attr, base_cls = attr.split(":")
            cls = getattr(obj, attr)
            attr = components.pop(0)

            # assume the base class is the first class matching by name
            for base in cls.mro():
                if base.__name__ == base_cls:
                    if attr.startswith("__"):
                        attr = "_%s%s" % (base_cls, attr)
                    obj = base.__dict__[attr].__get__(None, cls)
                    break
            else:
                raise Exception("%s is not a class base of %s: '%s'" % (base_cls, attr, name))
        else:
            obj = getattr(obj, attr)

    assert isinstance(obj, MDFNode)
    return obj

def _unpickle_context(cls, ctx_id, now, node_states, shift_sets):
    """
    returns an MDFContext from the pickled result of _pickle_context.

    Any shifted contexts are also loaded, and all node state in those
    contexts is set from the unpickled data.
    """
    node = cython.declare(MDFNode)
    node_state = cython.declare(NodeState)

    root = cls(now)

    # mapping of old ctx ids to new ids
    ctx_id_fixup = {ctx_id : root.get_id()}
    all_ctxs = {ctx_id : root}

    for shifted_ctx_id, shift_set in shift_sets:
        shifted_ctx = root.shift(shift_set)
        ctx_id_fixup[shifted_ctx_id] = shifted_ctx.get_id()
        all_ctxs[shifted_ctx_id] = shifted_ctx

    # get the node states and fixup the ctx id references
    for ctx_id, node, wrapper in node_states:
        node_state = wrapper.node_state
        node_state.ctx_id = ctx_id_fixup[node_state.ctx_id]

        if wrapper.alt_context_id in all_ctxs:
            node_state.alt_context = all_ctxs[wrapper.alt_context_id]

        if wrapper.prev_alt_context_id in all_ctxs:
            node_state.prev_alt_context = all_ctxs[wrapper.prev_alt_context_id]

        node_state_callers = node_state.callers
        node_state.callers = {}
        for caller_ctx_id, callers in node_state_callers.iteritems():
            new_caller_ctx_id = ctx_id_fixup[caller_ctx_id]
            node_state.callers[new_caller_ctx_id] = callers

        node_state_callees = node_state.callees
        node_state.callees = {}
        for callee_ctx_id, callees in node_state_callees.iteritems():
            new_callee_ctx_id = ctx_id_fixup[callee_ctx_id]
            node_state.callees[new_callee_ctx_id] = callees

        new_ctx_id = ctx_id_fixup[ctx_id]
        node._states[new_ctx_id] = node_state

    return root

def _pickle_node_state(node_state_wrapper):
    """
    returns a picklable tuple of args to be passed to _unpickle_node_state
    """
    node_state = cython.declare(NodeState)
    node_state = node_state_wrapper.node_state
    attribs = {}
    extra_attribs = {}

    # get the standard attributes that can be pickled as they are
    attribs["has_value"] = node_state.has_value
    attribs["date"] = node_state.date
    attribs["value"] = node_state.value
    attribs["called"] = node_state.called
    attribs["callers"] = node_state.callers
    attribs["callees"] = node_state.callees
    attribs["override"] = node_state.override

    # store context and override references as ids instead of objects
    if node_state.alt_context:
        extra_attribs["alt_context_id"] = node_state.alt_context.get_id()

    if node_state.prev_alt_context:
        extra_attribs["prev_alt_context_id"] = node_state.prev_alt_context.get_id()

    return (node_state.ctx_id, node_state.dirty_flags, attribs, extra_attribs)

def _unpickle_node_state(ctx_id, dirty_flags, attribs, additional_attribs):
    """
    returns a NodeStateWrapper object from the picked results of _pickle_node_state
    """
    node_state = NodeState(ctx_id, dirty_flags)
    node_state.has_value = attribs["has_value"]
    node_state.date = attribs["date"]
    node_state.value = attribs["value"]
    node_state.called = attribs["called"]
    node_state.callees = attribs["callees"]
    node_state.callers = attribs["callers"]
    node_state.override = attribs["override"]

    wrapper = NodeStateWrapper(node_state)
    for attr, value in additional_attribs.iteritems():
        setattr(wrapper, attr, value)            

    return wrapper

def _pickle_node(node):
    """
    returns a picklable tuple of args to be passed
    to _unpickle_node.
    
    Nodes don't have their state pickled. They are just picked
    so references to them can be stored.

    Any node state is pickled as part of the context.
    """
    # vardata was added in revision 7592
    if _pickle_version and _pickle_version < 7592:
        return (node.name, node._modulename, node._is_bound)

    # varnodes can be declared dynamically (e.g. sometimes this is done to create shift sets)
    # and so extra varnode data needs to be persisted so the node can be re-created.
    vardata = None
    if isinstance(node, MDFVarNode):
        varnode = cython.declare(MDFVarNode)
        varnode = node
        vardata = {
            "default" : varnode._default_value,
            "categories" : varnode.categories,
        }
  
    return (node.name, node._modulename, node._is_bound, vardata)

def _unpickle_node(node_name, modulename, is_bound, vardata=None):
    """returns an MDFNode object from the pickled results of _pickle_node"""
    # make sure all the required module is imported
    _log.debug("Unpickling node %s from module %s" % (node_name, modulename))
    if modulename:
        if modulename not in sys.modules:

            # TODO: we have to reference __builtin__ for now due to the way the import hook works
            # in the cluster. Get rid of it once a more stable import hook is implemented.
            __import__(modulename)

    # get the node and return it (there's no state to recover)
    try:
        return _get_node(node_name, is_bound)
    except MissingNodeError:
        if not vardata:
            raise

    # if the above block didn't return or raise then it must be a varnode
    # that hasn't been created (possibly one that was originally dynamically
    # created) so re-create it now.
    _log.debug("Creating new varnode node %s" % node_name)
    return MDFVarNode(name=node_name,
                      fqname=node_name,
                      default=vardata["default"],
                      category=vardata["categories"],
                      modulename=modulename)

def _pickle_custom_node(node, base_node, base_node_method_name, kwargs):
    """
    same as _pickle_node but with the addition of kwargs
    if the node was created using a MDFCustomNodeMethod.
    """
    if base_node is not None:
        return (node.name,
                node._modulename,
                node._is_bound,
                base_node,
                base_node_method_name,
                kwargs)

    return (node.name, node._modulename, node._is_bound, None, None, None)

def _unpickle_custom_node(node_name,
                          modulename,
                          is_bound,
                          base_node,
                          base_node_method_name,
                          kwargs):
    """returns MDFCustomNode from the pickled results of _pickle_custom_node"""
    if base_node is not None:
        # get the derived node by calling the method on the base node
        method = getattr(base_node, base_node_method_name)
        
        filter = kwargs.pop("filter", None)
        category = kwargs.pop("category", None)
        
        return method._get_derived_node(filter=filter,
                                        category=category,
                                        nodetype_func_kwargs=kwargs)

    return _unpickle_node(node_name, modulename, is_bound)

def _pickle_shift_set(shift_set):
    return (shift_set.items(),)

def _unpickle_shift_set(shift_set_items):
    return ShiftSet(shift_set_items)
