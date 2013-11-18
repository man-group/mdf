import uuid
import itertools
import logging
import inspect
import time
import sys
import cython
import copy
import types
import os
import re
from .parser import tokenize, get_assigned_node_name
from context import MDFContext, MDFNodeBase
from common import DIRTY_FLAGS

# these are cimported in nodes.pxd
# uncomment if not compiling with Cython
#from context import _get_current_context, _get_context, _profiling_enabled
#from cqueue import *

_logger = logging.getLogger(__name__)
_trace_enabled = cython.declare(int, False)

DIRTY_FLAGS_NONE = DIRTY_FLAGS.NONE
DIRTY_FLAGS_ALL  = DIRTY_FLAGS.ALL
DIRTY_FLAGS_TIME = DIRTY_FLAGS.TIME
DIRTY_FLAGS_ERR = DIRTY_FLAGS.ERR

# MethodWrapperType is missing from types
MethodWrapperType = type([].__delattr__)

def enable_trace(enable=True):
    global _trace_enabled
    _trace_enabled = enable

_pickle_node = None
_unpickle_node = None

def _lazy_imports():
    global _pickle_node, _unpickle_node
    import ctx_pickle
    _pickle_node = ctx_pickle._pickle_node
    _unpickle_node = ctx_pickle._unpickle_node


def _get_calling_module_and_class():
    """
    returns the first module on the call stack that's not this module
    If being called during class construction also returns the class name
    """
    this_module = sys.modules[__name__]
    this_package = this_module.__package__
    frame = sys._getframe() # frame = inspect.currentframe()
    try:
        while frame is not None:
            # try and avoid calling inspect.getmodule as it confuses the callstack
            # to break out of cython when evaluating nodes
            module = None
            if hasattr(frame, "__module__"):
                module = sys.modules.get(frame.__module__)
            else:
                # look for the module by filename
                filename = frame.f_code.co_filename
                if filename:
                    file_base, ext = os.path.splitext(filename)
                    for m in sys.modules.values():
                        if hasattr(m, "__file__"):
                            # ignore the .py/.pyc suffixes
                            m_base, ext = os.path.splitext(m.__file__)
                            if m_base == file_base:
                                module = m
                                break
            if getattr(module, "__package__", None) != this_package:
                clsname = frame.f_code.co_name
                if clsname == "<module>":
                    clsname = None
                return module, clsname
            frame = frame.f_back
    finally:
        # be careful not to create reference cycles
        # cypthon doesn't support deletion of local c variables,
        # but assigning it to None will have the same effect
        #del frame
        frame = None

    return None, None

if sys.version_info[0] > 2:
    import builtins
    TypeType = builtins.type
else:
    TypeType = types.TypeType

def _isgeneratorfunction(func):
    # see inpect.isgeneratorfunction
    # reproduced here to avoid breaking out of cython when
    # evaluating nodes
    if (isinstance(func, (types.FunctionType, types.MethodType)) 
    and func.__code__.co_flags & inspect.CO_GENERATOR):
        return True
    elif isinstance(func, TypeType) \
    and issubclass(func, MDFIterator):
        return True
    elif isinstance(func, MDFIteratorFactory):
        return True
    elif isinstance(func, MDFCallable):
        return func.is_generator()
    return False

def _is_member_of(cls, obj):
    """
    returns True if obj is a member of cls
    This has to be done without using getattr as it is used to
    check ownership of un-bound methods and nodes.
    """
    if obj in cls.__dict__.values():
        return True

    # check the base classes
    if cls.__bases__:
        for base in cls.__bases__:
            if _is_member_of(base, obj):
                return True

    return False

def _get_func_name(func):
    """returns a function's name"""
    try:
        return func.func_name
    except AttributeError:
        return func.__name__

def _get_module_name(module):
    """returns a module's name"""
    # multiprocessing imports the __main__ module into a new module called
    # __parents_main__ and then renames it. We need the modulename to
    # always be the same as the one in the parent process.
    if module.__name__ == "__parents_main__":
        return "__main__"
    return module.__name__

class NodeState(object):
    """
    context dependent state for MDFNode instances
    """

    def __init__(self, ctx_id, dirty_flags):
        self.ctx_id = ctx_id
        self.dirty_flags = dirty_flags
        self.has_value = False
        self.date = None
        self.value = None
        self.alt_context = None
        self.override = None
        self.override_cache = None

        # callers and callees are dicts mapping ctx_id
        # to nodes. A node in a context is dependent 
        # on other nodes in contexts, rather than just
        # a simple graph of nodes to nodes.
        self.callers = {}
        self.callees = {}

        self.depends_on_cache = {}
        self.add_dependency_cache = set()

        # bools to indicate which context callbacks have been set/unset
        # to reduce the number of dictionary set calls required
        self.ctx_set_date_callback_done = False
        self.ctx_incremental_update_callback_done = False

        # this is used in _set_dirty as the size of the queue required
        # will be the same for each iteration and this avoid re-allocating
        # it each time
        self.set_dirty_queue = cqueue(0)

        # additional members only used by evalnode methods
        self.called = False
        self.prev_alt_context = None
        self.generator = None

    def __repr__(self):
        return "<NodeState>" + "\n\t".join([
            "ctx_id: %s" % self.ctx_id,
            "dirty_flags: %s" % self.dirty_flags,
            "has_value: %s" % self.has_value,
            "called: %s" % self.called,
            "date: %s" % self.date,
            "value: %s" % self.value,
            "alt_context: %s" % self.alt_context,
            "prev_alt_context: %s" % self.prev_alt_context,
            "override: %s" % self.override,
            "override_cache: %s" % self.override_cache,
            "callers: %s" % ( 
                ("\n\t\t" +
                "\n\t\t".join("<ctx %d> : %s" % (
                    k, "\n\t\t\t".join([n.name for n in v])) for k, v in self.callers.iteritems()))
                if self.callers else ""
            ),
            "callees: %s" % ( 
                ("\n\t\t" +
                "\n\t\t".join("<ctx %d> : %s" % (
                    k, "\n\t\t\t".join([n.name for n in v])) for k, v in self.callees.iteritems()))
                if self.callees else ""
            ),
        ]) + "\n</NodeState>"

class MDFIterator(object):
    """
    MDFIterator is used as a way of writing path-dependent evalnodes.
    
    Usually an evalnode is simply a function that's called whenever
    required and retains no state from its previous invocation.
    
    If an MDFIterator is used instead of a function instances of it
    will be instanciated when required and then iterated over
    to get values as time is advanced.
    
    eg::
    
        @evalnode
        class my_timedepedent_node(MDFIterator):
            def __init__(self):
                self.i = 0

            def next(self):
                self.i += 1
                return self.i

        ctx = MDFContext(datetime(1990, 1, 1))
        print ctx[my_timedepedent_node]
        >> 1

        ctx.set_date(datetime(1990, 1, 2))
        print ctx[my_timedepedent_node]
        >> 2
        
    NOTE: the node is accessed just like any other, as shown above,
          and so it is appropriate to use the same naming convention
          as for functions, even though it is implemented as a class.
    """

    def __init__(self, cls=None):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        raise NotImplementedError("next")

class MDFIteratorFactory(object):
    """
    Callable object that returns an instance of MDFIterator.
    Used by custom node types that have eval functions that
    produce MDFIterators but need a non-empty ctor args list.
    
    Also used when MDFIterator nodes are used as class nodes.
    A the factory object is used as the bound form of the
    iterator class, which constructs the iterator object
    with the bound class when the node is evaluated. 
    """

    def __init__(self, iterator_class, node_class):
        self.iterator_class = iterator_class
        self.node_class = node_class

    def __call__(self):
        return self.iterator_class(self.node_class)

class MDFCallable(object):
    """
    For internal use by cythoned code where we need to construct
    something that looks like a node function, rather than a
    cythoned function.

    The call method takes no args as this is used as a node
    function and so none are required.
    """
    def __init__(self, name, callable_, *func_chain):
        self.func_name = name
        self.callable = callable_
        self.func_chain = func_chain

    def __call__(self):
        result = self.callable()
        for func in self.func_chain:
            result = func(result)
        return result

    def __get__(self, instance, owner):
        func = self.callable

        if isinstance(func, types.FunctionType):
            if func.__code__.co_argcount == 0:
                func = staticmethod(func).__get__(instance, owner)

            if func.__code__.co_argcount == 1:
                func = classmethod(func).__get__(instance, owner)

        elif isinstance(func, (classmethod, staticmethod, MDFCallable, MDFEvalNode)):
            func = func.__get__(instance, owner)

        return MDFCallable(self.func_name, func, *self.func_chain)

    def is_generator(self):
        return _isgeneratorfunction(self.callable)

class ConditionalDependencyError(Exception):
    
    def __init__(self, node, ctx, alt_ctx, new_alt_ctx):
        # get the nodes that the new alt context is shifted by that
        # the previous one wasn't.
        new_node_shifts = new_alt_ctx.get_shift_set().keys()
        prev_node_shifts = alt_ctx.get_shift_set().keys()
        diff_nodes = set(new_node_shifts) - set(prev_node_shifts)

        # render the partial dependency graph as a tree to show which nodes
        # are dependent on the nodes in the new alt context that weren't in the
        # previous context. 
        seen = set()
        def flatten_dependency_tree(node, ctx, indent_level=1):
            dep_tree = []
            if (node, ctx) in seen:
                return dep_tree
            seen.add((node, ctx))

            for called_node, called_ctx in node.get_dependencies(ctx):
                traverse = False
                for diff_node in diff_nodes:
                    if called_node.depends_on(called_ctx, diff_node, alt_ctx):
                        traverse = True
                        break

                if traverse:
                    display_name = called_node.name
                    if called_node in diff_nodes:
                        display_name = "** %s **" % display_name
                    dep_tree.append(("  " * indent_level) + "-> " + display_name)
                    dep_tree.extend(flatten_dependency_tree(called_node,
                                                            called_ctx,
                                                            indent_level+1))
            return dep_tree

        dep_tree = [node.name] + flatten_dependency_tree(node, alt_ctx)
        dep_tree =  "\n".join(dep_tree)

        Exception.__init__(self,
                   (("Error evaluating %s[%s]\n\n" % (node.name, ctx)) +
                    ("Evaluated %s in %s, but a dependency has " % (node.name, alt_ctx)) +
                    ("changed that requires the shifted context %s " % new_alt_ctx) +
                    ("to be used instead.\n") +
                    ("This is most likely be due to a conditional dependency "
                     "on one or more of the following nodes: \n\n%s" % 
                        "\n".join(["  %s" % x.name for x in diff_nodes])) +
                    ("\n\nMake that dependency unconditional to fix.") +
                    ("\n\n%s" % dep_tree)))

class MDFNode(MDFNodeBase):
    """
    MDFNode is a base class for other types of node or may be
    used as a leaf node.
    """
    _additional_attrs_ = {}

    # subclasses can choose to mask out certain dirty flags when propagated
    # from called nodes
    dirty_flags_propagate_mask = DIRTY_FLAGS_ALL

    # subclasses can use this to indicate whether a node is
    # dirty or not by default
    @property
    def _default_dirty_flags_(self):
        return DIRTY_FLAGS_ALL

    def __init__(self,
                 name=None,
                 short_name=None,
                 cls=None,
                 fqname=None,
                 category=None,
                 modulename=None):
        assert name or fqname
        self._name = fqname or name
        self._short_name = short_name
        self._modulename = modulename
        self._is_bound = cls is not None
        self._categories = tuple(category if isinstance(category, (list, tuple)) else [category])
        self._has_on_dirty_callback = hasattr(self, "on_set_dirty")
        self._has_set_date_callback = hasattr(self, "on_set_date")
        self._has_timestep_update = False
        self._dirty_flags_propagate_mask = self.dirty_flags_propagate_mask

        # derived nodes are nodes that are derived from this one via the special methods added to
        # _addition_atts_ by custom node types. See MDFCustomNodeMethod.
        self._derived_nodes = {}

        # get module name for this node and set the fully qualified name if not passed in
        if cls is not None:
            base_cls = cls
            if isinstance(cls, tuple):
                cls, base_cls = cls

            module = None
            if hasattr(cls, "__module__"):
                module = sys.modules.get(cls.__module__)
            if module is None:
                module = inspect.getmodule(cls)
            self._modulename = modulename or _get_module_name(module)
            if fqname is None:
                self._name = "%s.%s:%s.%s" % (self._modulename,
                                                cls.__name__,
                                                base_cls.__name__,
                                                name)
        else:
            module, clsname = _get_calling_module_and_class()
            if module is not None or clsname is not None:
                if self._modulename is None:
                    if module is None:
                        # this can happen if the node is declared in the main module and is being
                        # run through the profiler, or if the node is declared in an ipython notebook.
                        self._modulename = "__unknown__"
                    else:
                        self._modulename = _get_module_name(module)

                if fqname is None:
                    if clsname:
                        self._name = "%s.%s.%s" % (self._modulename, clsname, name)
                    else:
                        self._name = "%s.%s" % (self._modulename, name)

        # dict of NodeStates per context
        self._states = {}
        MDFContext.register_node(self)

    def __str__(self):
        return "<%s [name=%s]>" % (self.__class__, self._name)

    def __repr__(self):
        return "<%s [name=%s] at 0x%x>" % (self.__class__, self._name, id(self))

    def __reduce__(self):
        """support for pickling"""
        return (
            _unpickle_node,
            _pickle_node(self),
            None,
            None,
            None,
        )

    def __getattr__(self, attr):
        # custom node types add additional methods to the _additional_attrs_ dict
        # that are returned here.
        # They're not simply added into the class's __dict__ since it's an extension type
        # and so the type itself is immuatable.
        try:
            value = self._additional_attrs_[attr]
            try:
                return value.__get__(self)
            except AttributeError:
                pass
            return value
        except KeyError:
            raise AttributeError(attr)

    # 
    # Hook in arithmetic operators. (These are added to _additional_attrs_ in nodetypes.py)
    # NB: Cython arithmetic operator methods behave differently; there are no __r*__ variants.
    # See: http://docs.cython.org/src/userguide/special_methods.html#arithmetic-methods
    # 
    def __add__(lhs, rhs):
        return MDFNode._commutative_binop("__add__", lhs, rhs)
    
    def __mul__(lhs, rhs):
        return MDFNode._commutative_binop("__mul__", lhs, rhs)
    
    def __sub__(lhs, rhs):
        if isinstance(lhs, MDFNode):
            return MDFNode._op("__sub__", lhs, rhs)
        return -rhs + lhs
    
    def __div__(lhs, rhs):
        if isinstance(lhs, MDFNode):
            return MDFNode._commutative_binop("__div__", lhs, rhs)
        return (_one_node / rhs) * lhs

    def __truediv__(lhs, rhs):
        if isinstance(lhs, MDFNode):
            return MDFNode._commutative_binop("__truediv__", lhs, rhs)
        return (_one_node / rhs) * lhs
    
    def __neg__(self):
        return MDFNode._op("__neg__", self)
    
    @staticmethod
    def _commutative_binop(op_name, lhs, rhs):
        """
        For commutative operators where order doesn't matter (e.g. addition: x + y == y + x)
        we can just swap the arguments to handle the reversed case, where lhs is not a node.
        """
        if not isinstance(lhs, MDFNode):
            lhs, rhs = rhs, lhs
        return MDFNode._op(op_name, lhs, rhs)
        
    @staticmethod
    def _op(op_name, lhs, rhs=None):
        """
        Implements a unary and binary operators.
        NB: lhs must always be an MDFNode.
        """
        try:
            binop = lhs._additional_attrs_[op_name]
        except KeyError:
            raise AttributeError
        return binop.__get__(lhs)(rhs)

    @property
    def categories(self):
        """the categories this node belongs to"""
        return self._categories

    def get_dependencies(self, ctx):
        """
        returns a list of (node, ctx) that this node is dependent on
        in the context
        """
        node_state = self._get_state(ctx)
        results = []
        for ctx_id, callees in node_state.callees.iteritems():
            ctx = _get_context(ctx_id, ctx)
            for callee in callees:
                results.append((callee, ctx))
        return results

    def _get_state(self, ctx):
        """returns the NodeState object for this node and context"""
        try:
            return self._states[ctx._id_obj]
        except KeyError:
            pass

        # otherwise create a new state for this context and return it
        state = self._states[ctx._id_obj] = NodeState(ctx._id_obj, self._default_dirty_flags_)
        return state

    def get_state(self, ctx):
        """
        Return the NodeState object for this node and context.
        
        This is for debug purposes only. The NodeState object returned
        is immutable and the members are not accessible outside of this
        module intentionally. It can however be printed if debug information
        about the state of the node in a context is required.
        """
        # public API version of _get_state
        # doesn't create a state if one doesn't already exist.
        try:
            return self._states[ctx._id_obj]
        except KeyError:
            return None 

    def get_callers(self, ctx):
        """
        returns a list of (node, ctx) of nodes that dependent on this node
        """
        node_state = self._get_state(ctx)
        results = []
        for ctx_id, callers in node_state.callers.iteritems():
            ctx = _get_context(ctx_id, ctx)
            for callee in callers:
                results.append((callers, ctx))
        return results        

    def clear(self, ctx):
        """
        clears any cached state for a context

        This clears dependencies as well as values and should usually only
        be called when a context is deleted.
        """
        try:
            del self._states[ctx._id_obj]
        except KeyError:
            pass

    def clear_value(self, ctx):
        """
        clears any currently set value for this node
        """
        node_state = self._get_state(ctx)
        node_state.has_value = False
        node_state.value = None
        node_state.generator = None

        # clearing the value also marks the node as dirty
        self._set_dirty(node_state, DIRTY_FLAGS_ALL, 0)

    def add_dependency(self, ctx, called_node_, called_ctx):
        """
        adds a dependency to the node in a context to another node
        and the context that node has been called in.

        read as: self[ctx] has called called_node[called_ctx]
        """
        return self._add_dependency(ctx, called_node_, called_ctx)

    def _add_dependency(self, ctx, called_node_, called_ctx):
        """
        cdef implementation of py:func:`MDFNode.add_dependency`
        """
        # called_node_ is an MDFNodeBase
        called_node = cython.declare(MDFNode)
        called_node = called_node_

        node_state = self._get_state(ctx)

        if (called_node, called_ctx._id_obj) in node_state.add_dependency_cache:
            return

        # add the called node to this node's callees
        node_state.callees.setdefault(called_ctx._id_obj, set()).add(called_node)

        # add this node to the called nodes callers
        called_state = called_node._get_state(called_ctx)
        called_state.callers.setdefault(ctx._id_obj, set()).add(self)

        if _trace_enabled:
            _logger.info("Updated dependency: %s[%s] -> %s[%s]" % (
                                                           self.name,
                                                           ctx,
                                                           called_node.name,
                                                           called_ctx))

        # don't add this dependency again
        node_state.add_dependency_cache.add((called_node, called_ctx._id_obj))

        self._clear_dependency_cache(ctx)

    def _clear_dependency_cache(self, ctx):
        # get all the contexts that derive from this one
        # If any node has an alt_context in any of these
        # contexts it should be invalidated as the change
        # in dependency could affect whether that alt_context
        # can still be used or not.
        node_state = cython.declare(NodeState)
        other_state = cython.declare(NodeState)
        caller = cython.declare(MDFNode)

        parent = ctx.get_parent() or ctx
        shifted_contexts = parent.get_shifted_contexts()

        node_state = self._states[ctx._id_obj]
        to_clear = cqueue()
        cqueue_push(to_clear, (self, node_state))

        seen = set()
        while len(to_clear) > 0:
            caller, node_state = cqueue_popleft(to_clear)
            if node_state in seen:
                continue
            seen.add(node_state)

            # clear the dependency cache and alt_context
            node_state.depends_on_cache.clear()
            node_state.override_cache = None
            node_state.alt_context = None

            # clear any alt_contexts set in any of shifted contexts that
            # have their alt state set to this one
            for other_state in caller._states.itervalues():
                if other_state.alt_context is not None \
                and other_state.alt_context._id == node_state.ctx_id:
                    other_state.alt_context = None

            # add any nodes that called this one to the list to be cleared
            for ctx_id, callers in node_state.callers.iteritems():
                for caller in callers:
                    node_state = caller._states[ctx_id]
                    cqueue_push(to_clear, (caller, node_state))

    def depends_on(self, ctx, other_node, other_ctx):
        node_state = cython.declare(NodeState)
        try:
            node_state = self._states[ctx._id_obj]
        except KeyError:
            # if there's no node state, it can't depend on any other node
            return False
        return self._depends_on(node_state, other_node, other_ctx._id_obj)

    def _depends_on(self, node_state, other, other_ctx_id):
        """
        returns True of other appears in this nodes dependency
        graph either directly or indirectly.

        Nodes are dependent on themselves for the purpose of this
        function.

        read as: self[node_state.ctx] depends on other[other_ctx]
        """
        try:
            return node_state.depends_on_cache[(other, other_ctx_id)]
        except KeyError:
            pass

        # do a breadth first search of the graph to find any callee
        # that matches the other node
        remaining_callees = cqueue()
        cqueue_push(remaining_callees, (node_state.ctx_id, self))

        seen = set()
        callee = cython.declare(MDFNode)
        callee_state = cython.declare(NodeState)
        ctx_id = cython.declare(int)
        while cqueue_len(remaining_callees) > 0:
            ctx_id, callee = remaining_callees.popleft()

            if (ctx_id, callee) in seen:
                continue
            seen.add((ctx_id, callee))

            if ctx_id == other_ctx_id and callee is other:
                node_state.depends_on_cache[(other, other_ctx_id)] = True
                return True

            try:
                callee_state = callee._states[ctx_id]
            except KeyError:
                continue

            # add the callees of this node to the search
            for ctx_id, callees in callee_state.callees.iteritems():
                for callee in callees:
                    cqueue_push(remaining_callees, (ctx_id, callee))

        node_state.depends_on_cache[(other, other_ctx_id)] = False
        return False

    @property
    def node_type(self):
        """returns the name of the node type of this node"""
        return "node"

    @property
    def name(self):
        """fully qualified name of this node, including module and class names"""
        return self._name

    @property
    def is_bound(self):
        """True if this node is bound to a class"""
        return self._is_bound

    @property
    def short_name(self):
        """
        name of this node, excluding the module name and only showing the bound class
        for class nodes, not the class the method is declared on.
        """
        if self._short_name is not None:
            return self._short_name

        name = self._name
        if self._modulename:
            prefix = "%s." % self._modulename
            if name.startswith(prefix):
                name = name[len(prefix):]
        self._short_name = ".".join([x.split(":")[0] for x in name.split(".")])
        return self._short_name

    @property
    def modulename(self):
        """name of the module this node was declared in"""
        return self._modulename

    def has_timestep_update(self, ctx):
        """returns True if the node value can be updated incrementally as time is updated"""
        return False

    def is_dirty(self, ctx, mask=DIRTY_FLAGS_ALL):
        """
        returns the dirty flags for this node

        any value other than DIRTY_FLAGS.NONE indicates a dependency
        has been modified since the node was last valued.
        """
        node_state = self._get_state(ctx)
        return node_state.dirty_flags & mask

    def set_dirty(self, ctx, flags=DIRTY_FLAGS_ALL):
        node_state = self._get_state(ctx)
        int_flags = cython.declare(int, flags)
        return self._set_dirty(node_state, int_flags, 0)

    def _set_dirty(self, node_state, flags, _depth):
        """
        Updates the dirty flags on this node.

        Marking a node as dirty automatically marks all nodes
        dependent on this node as dirty.
        """
        node = cython.declare(MDFNode)
        caller = cython.declare(MDFNode)
        obj = cython.declare(NodeSetDirtyState)
        to_process = cython.declare(cqueue)

        # start off with a reasonable amount of space and just one entry
        to_process = node_state.set_dirty_queue
        cqueue_clear(to_process)
        
        # NodeSetDirtyState objects are used instead of a tuple to avoid having to
        # keep converting native types to python types
        cqueue_push(to_process, create_NodeSetDirtyState(_depth, node_state, self, flags))

        depth = cython.declare(int, _depth)
        while cqueue_len(to_process) > 0:
            # pop the current item of the left of the queue
            obj = cqueue_popleft(to_process)
            depth, node_state, node, flags = obj.depth, obj.node_state, obj.node, obj.flags

            # if propagating dirty flags from one node to the calling nodes
            # mask the propagated dirty flags by the calling node's mask
            # as it may want to ignore some flags (this also stops them being
            # propagated further up the graph).
            if depth > 0:
                flags = flags & node._dirty_flags_propagate_mask

            # do nothing if all the dirty flags are already set
            if (node_state.dirty_flags & flags) == flags:
                if _trace_enabled:
                    ctx = _get_context(node_state.ctx_id)
                    _logger.info("%s%s[%s] already dirty (%s)" % (
                                    ("-" * depth) + "> " if depth else "",
                                    node.name,
                                    ctx,
                                    DIRTY_FLAGS.to_string(node_state.dirty_flags)))
                continue

            if _trace_enabled:
                ctx = _get_context(node_state.ctx_id)
                _logger.info("%s%s[%s] marked dirty (%s)" % (
                                ("-" * depth) + "> " if depth else "",
                                node.name,
                                ctx,
                                DIRTY_FLAGS.to_string(flags)))

            # update the dirty flag
            node_state.dirty_flags |= flags
            if node._has_on_dirty_callback:
                ctx = _get_context(node_state.ctx_id)
                node.on_set_dirty(ctx, flags)

            # remove any cached value as it will have to be re-calculated
            if flags & ~DIRTY_FLAGS_TIME:
                node_state.has_value = False
                node_state.value = None

            # add this node's callers to the list to process
            for ctx_id, callers in node_state.callers.iteritems():
                for caller in callers:
                    try:
                        caller_state = caller._states[ctx_id]
                        cqueue_push(to_process, create_NodeSetDirtyState(depth + 1, caller_state, caller, flags))
                    except KeyError:
                        continue

    def touch(self, ctx, flags=DIRTY_FLAGS_ALL):
        node_state = self._get_state(ctx)
        int_flags = cython.declare(int, flags)
        self._touch(node_state, int_flags, False)

    def _touch(self, node_state, flags=DIRTY_FLAGS_ALL, _quiet=False, _depth=0):
        """
        Mark this node as not dirty and all calling nodes as dirty.
        
        All shifted contexts that also share this node are also touched.

        If _quiet is True only this node is touched (in ctx and possibly
        in any shifted contexts) and but nodes dependent on this one
        are not dirtied.
        """
        if _trace_enabled:
            ctx = _get_context(node_state.ctx_id)
            _logger.info("%s[%s] touched (%s)" % (
                            self.name,
                            ctx,
                            DIRTY_FLAGS.to_string(flags)))

        # clear the flags
        node_state.dirty_flags &= ~flags

        if not _quiet:
            # mark any calling nodes as dirty
            caller = cython.declare(MDFNode)
            caller_state = cython.declare(NodeState)
            for ctx_id, callers in node_state.callers.iteritems():
                for caller in callers:
                    try:
                        caller_state = caller._states[ctx_id]
                        caller._set_dirty(caller_state, flags, _depth+1)
                    except KeyError:
                        continue

    # thread_id is passed in to avoid fetching it again later if this has to get another node value
    def get_value(self, ctx, thread_id=None):
        """
        returns the value for this node for a given context.

        Depending on the node type this may involve calculating
        the latest value.
        """
        node_state = cython.declare(NodeState)
        node_state = self._get_state(ctx)

        # return the cached value if the node isn't dirty
        if node_state.dirty_flags == DIRTY_FLAGS_NONE:
            if _trace_enabled:
                _logger.debug("Have cached value for %s[%s]" % (self.name, ctx))
            return self._get_cached_value_and_date(ctx, node_state)[0]

        # get the alt context this node should be evaluated in (i.e. the least shifted context
        # with all the shifts this node depends on).
        alt_ctx = cython.declare(MDFContext)
        alt_ctx = self.get_alt_context(ctx)

        # check the alt_ctx hasn't changed if it's been reset since last time
        if node_state.prev_alt_context is not None and node_state.prev_alt_context is not alt_ctx:
            raise ConditionalDependencyError(self, ctx, node_state.prev_alt_context, alt_ctx)

        try:
            # get the value from alt_ctx if its different from the current context
            if alt_ctx is not ctx:
                return alt_ctx._get_node_value(self, self, ctx, thread_id)

            override = cython.declare(MDFNode)
            override = self._get_override(ctx, node_state)
            if override is not None:
                value = ctx._get_node_value(override, self, ctx, thread_id)
                self._set_value(ctx, node_state, value)
                return value

            # otherwise call the subclass's _get_value method
            value = self._get_value(ctx, node_state)
            self._set_value(ctx, node_state, value)
            return value
        except:
            # Clear all dirty flags
            self._touch(node_state, DIRTY_FLAGS_ALL, True)

            # Set the error flag
            self._set_dirty(node_state, DIRTY_FLAGS_ERR, 0)
            
            # and re-raise
            raise

        finally:
            # If nothing's changed this is a cheap operation as the alt_context is cached
            new_alt_ctx = cython.declare(MDFContext)
            new_alt_ctx = self.get_alt_context(ctx)

            # check the alt_context hasn't changed after getting the value
            # on the alt_context. This could happen is a new dependency was
            # introduced that made this node dependent on any of the shifted
            # nodes between ctx and alt_ctx.
            if alt_ctx is not ctx and new_alt_ctx is not alt_ctx:
                # if this error happens often it might be worth just re-evaluating in the
                # new alt_ctx, but that would result in wasted valuations that could be
                # avoided in most cases I imagine.
                raise ConditionalDependencyError(self, ctx, alt_ctx, new_alt_ctx)

            # remember the alt_context for next time
            node_state.prev_alt_context = new_alt_ctx

    def _get_value(self, ctx, node_state):
        """
        returns the value for this node for a given context.

        This method is also responsible for caching the value
        by calling self.set_value or self.set_exception
        if the value can be re-used next time.

        Depending on the node type this may involve calculating
        the latest value.

        *override in subclass*
        """
        raise NotImplementedError("_get_value")

    def _get_cached_value_and_date(self, ctx, node_state):
        """
        returns the cached value and date for this node in a context
        """
        if not node_state.has_value:
            raise KeyError("%s not found in %s" % (self.name, ctx))

        return node_state.value, node_state.date

    def _get_cached_value(self, ctx):
        """
        returns just the cached value - used by MDFContext.__str__
        *doesn't check a value exists*
        """
        node_state = self._get_state(ctx)
        return node_state.value

    def get_alt_context(self, ctx):
        """
        returns the context values for this node[ctx] actually
        belong in. This can be different from ctx if its a
        shifted context but this node doesn't depend on that shift.
        """
        node_state = self._get_state(ctx)
        if node_state.alt_context is None:
            node_state.alt_context = self._get_alt_context(ctx)
        return node_state.alt_context

    def _reset_alt_context(self, ctx):
        """
        Resets the remembered alt context if one has been set.
        """
        node_state = self._get_state(ctx)
        node_state.alt_context = None
        node_state.prev_alt_context = None

    def _get_alt_context(self, ctx):
        raise Exception("must be implemented in subclass")

    def has_value(self, ctx):
        """
        returns True if a cached value exists in this context.
        """
        node_state = cython.declare(NodeState)
        try:
            node_state = self._states[ctx._id_obj]
            return node_state.has_value
        except KeyError:
            return False

    def was_called(self, ctx):
        """
        returns True if this node has ever been called in this context
        """
        node_state = cython.declare(NodeState)
        try:
            node_state = self._states[ctx._id_obj]
            return node_state.called
        except KeyError:
            return False

    def _value_setter(self, value):
        ctx = _get_current_context()
        self.set_value(ctx, value)

    def _value_getter(self):
        return self.__call__()
    
    value = property(_value_getter, _value_setter)

    def set_value(self, ctx, value):
        """
        sets the value of this node for a given context
        """
        node_state = self._get_state(ctx)
        self._set_value(ctx, node_state, value, False)

    def _set_value(self, ctx, node_state, value, _quiet=True):
        """
        called by get_value to stored the cached value

        If _quiet is True the node will be marked as not dirty but
        the calling nodes won't be notified. This is the default
        as this method is usually used to set the value in response
        to a recalculation due to the flags being set, and they
        will already have been set on the calling nodes so there's no
        need to re-set them.
        """
        # set the value
        node_state.has_value = True
        node_state.date = ctx._now
        node_state.value = value

        # touch the node to reset the flags and touch and callers
        self._touch(node_state, DIRTY_FLAGS_ALL, _quiet)

    def _override_getter(self):
        ctx = _get_current_context()
        node_state = self._get_state(ctx)
        return node_state.override

    def _override_setter(self, override_node):
        ctx = _get_current_context()
        self.set_override(ctx, override_node)

    override = property(_override_getter, _override_setter)

    def set_override(self, ctx, override_node):
        """
        Sets an override node for this node in this context.
        
        Once an override is set this node delegates its evaluation
        to the override_node whenever its evaluated in ``ctx``.
        """
        shift_set = ctx.get_shift_set()
        if shift_set and override_node is not shift_set.get(self, None):
            raise Exception("Nodes can only be overriden in the root context, "
                            "or if the context is shifted by the node being overriden")       

        # get the root context the override is being applied to
        root_ctx = cython.declare(MDFContext)
        root_ctx = ctx.get_parent() or ctx
        if self in shift_set:
            # the the context only shifted by this node (ctx could be shifted by other nodes as well)
            root_ctx = root_ctx.shift({self : override_node})

        # early out if the context is already shifted by override_node
        node_state = cython.declare(NodeState)
        node_state = self._get_state(root_ctx)
        if node_state.override is override_node:
            return

        # set the override node to be used by get_value
        node_state.override = override_node

        # reset the state for the root context and all shifts of it
        all_contexts = set([root_ctx])
        if root_ctx.get_parent() is not None:
            all_contexts.update(root_ctx.get_parent().get_shifted_contexts())

        for ctx in all_contexts:
            if ctx is root_ctx \
            or ctx.is_shift_of(root_ctx):
                # let any dependencies know the value of this node is invalid
                self.set_dirty(ctx, DIRTY_FLAGS_ALL)

                # clear any cached dependencies as they've changed
                self._clear_dependency_cache(ctx)

                try:
                    node_state = self._states[ctx._id_obj]
                    node_state.alt_context = None
                    node_state.prev_alt_context = None
                    node_state.override_cache = None
                    node_state.callees.clear()
                    node_state.add_dependency_cache.clear()
                except KeyError:
                    pass

    def _get_override(self, ctx, node_state):
        # if called for this context previously return the cached result
        if node_state.override_cache is not None:
            if node_state.override_cache is self:
                return None
            return node_state.override_cache

        # if there's an override set for this node in this context return that
        if node_state.override is not None:
            node_state.override_cache = node_state.override
            return node_state.override

        # find the most shifted context with an override set between ctx
        # and the root context
        parent = cython.declare(MDFContext)
        parent = ctx.get_parent()
        if parent is None:
            node_state.override_cache = self
            return None

        best_match_num_shifts = cython.declare(int)
        best_match_num_shifts = -1

        best_match = cython.declare(MDFContext)
        best_match = None

        shifted_ctx = cython.declare(MDFContext)
        shifted_node_state = cython.declare(NodeState)
        for shifted_ctx in itertools.chain([parent], parent.get_shifted_contexts()):
            try:
                shifted_node_state = self._states[shifted_ctx._id_obj]
            except KeyError:
                continue

            if shifted_node_state.override is not None \
            and (shifted_ctx is ctx or ctx.is_shift_of(shifted_ctx)):
                # shifted_ctx is a shifted version of ctx and has an
                # override set. If it's more shifted than any previously
                # encountered use the shift from this context
                num_shifts = len(shifted_ctx.get_shift_set())

                if num_shifts == best_match_num_shifts:
                    # if two equally shifted contexts both have an override set for
                    # this context then there's no way to sensibly decide which
                    # override to use. This won't usually happen as overrides are
                    # either set on the root context or as a result of shifting.
                    raise Exception("Ambiguous override found for %s: %s vs. %s" % (self.name,
                                                                                    best_match,
                                                                                    shifted_ctx))

                if num_shifts > best_match_num_shifts:
                    best_match = shifted_ctx
                    best_match_num_shifts = num_shifts

                    # early out if this context is the main context
                    # since there can't be a candidate that ctx is a
                    # shift of and is more shifted
                    if shifted_ctx is ctx:
                        break

        # if no contexts with an override were found return None
        if best_match is None:
            node_state.override_cache = self
            return None

        shifted_node_state = self._states[best_match._id_obj]
        node_state.override_cache = shifted_node_state.override
        return shifted_node_state.override

    def get_override(self, ctx):
        """
        returns the override that will be used to evaluate this node in ctx, or None
        """
        node_state = self._get_state(ctx)
        return self._get_override(ctx, node_state)

    def __call__(self):
        """set up the context and return the value for this node"""
        if _profiling_enabled:
            stop_time = time.clock()

        ctx_ = cython.declare(MDFContext)
        ctx_ = _get_current_context()

        if _profiling_enabled:
            timer = ctx_._pause_current_timer(stop_time)

        try:
            # always get the value via the context so any new dependencies get set up
            return ctx_._get_node_value(self)
        finally:
            if _profiling_enabled:
                timer.resume()

@cython.cclass 
class NodeSetDirtyState(object):
    """internal class used by MDFNode._set_dirty"""
    depth = cython.declare(int)
    node_state = cython.declare(NodeState)
    node = cython.declare(MDFNode)
    flags = cython.declare(int)

# cfunc to create NodeSetDirtyState to avoid having to convert the args
# into a python tuple as would happen if using __init__
@cython.cfunc
@cython.locals(depth=int, node_state=NodeState, node=MDFNode, flags=int)
@cython.returns(NodeSetDirtyState)
def create_NodeSetDirtyState(depth, node_state, node, flags):
    obj = cython.declare(NodeSetDirtyState)
    obj = NodeSetDirtyState()
    obj.depth = depth
    obj.node_state = node_state
    obj.node = node
    obj.flags = flags
    return obj

class MDFVarNode(MDFNode):
    """most basic type of node that just holds a value"""
    _no_default_value_ = object()

    def __init__(self,
                 name,
                 default=_no_default_value_,
                 cls=None,
                 fqname=None,
                 category=None,
                 modulename=None):
        self._default_value = default
        MDFNode.__init__(self, name=name, cls=cls, fqname=fqname, category=category, modulename=modulename)

    @property
    def node_type(self):
        """returns the name of the node type of this node"""
        return "varnode"

    def _get_value(self, ctx, node_state):
        try:
            return self._get_cached_value_and_date(ctx, node_state)[0]
        except KeyError:
            if self._default_value is self._no_default_value_:
                raise

        return self._default_value

    def _get_alt_context(self, ctx):
        """
        returns the context values for this node[ctx] actually
        belong in. This can be different from ctx if its a
        shifted context but this node doesn't depend on that shift.
        """
        ctx_shift_set = ctx.get_shift_set()
        shift_set = {}
        if self in ctx_shift_set:
            shift_set[self] = ctx_shift_set[self]

        parent = cython.declare(MDFContext)
        parent = ctx.get_parent() or ctx
        return parent.shift(shift_set)

def varnode(name=None, default=MDFVarNode._no_default_value_, category=None):
    """
    Creates a simple :py:class:`MDFNode` that can have a value assigned
    to it in a context.
    
    It may also take a default value that will be used if no specific
    value is set for the node in a context.
    
    A varnode may be explicitly named using the name argument, or
    if left as None the variable name the node is being assigned to
    will be used.

    ::

        my_varnode = varnode(default=100)

    """
    if name is None:
        name = get_assigned_node_name("varnode", 0 if cython.compiled else 1)
    return MDFVarNode(name, default, category=category)

class _VarGroupMeta(type):
    """
    Metaclass for constructing vargroups. At the moment, the main purpose
    is aiding pretty printing.
    """

    def __new__(cls, cls_name, bases=None, dict=None, **kwargs):
        return super(_VarGroupMeta, cls).__new__(cls, cls_name, bases, dict)

    def __init__(self, cls_name, bases=None, dict=None, group_name=None):
        self._dict = dict
        self._group_name = group_name
        super(_VarGroupMeta, self).__init__(cls_name, bases, dict)

    def __repr__(self):
        return "<%s, %s>" % (self._group_name or "vargroup", self._dict)

def vargroup(group_name=None, **kwargs):
    """
    Converts the arguments provided as kwargs to :py:class:`MDFVarNode` objects
    and groups them in a single object. If the group_name parameter is
    supplied, the category of the resulting nodes will be set to it's value.

    The nodes are accessible as attributes of the returned object:

    eg::

        >>> params = vargroup("input_params", attr1=5, attr2=None)
        >>> print params
        <vargroup=input_params, {'attr2': <<type 'MDFVarNode'> ...>, 'attr1': <<type 'MDFVarNode'> ...>}>
        >>> print params.attr2
        <<type 'MDFVarNode'> ...>
        >>> params.attr2()
        5
    """
    attribs = dict([(k, varnode(k, default=v, category=group_name)) for k, v in kwargs.iteritems()])
    cls_name = "%s__%s" % (group_name, str(uuid.uuid4()))
    return _VarGroupMeta(cls_name, bases=(object,), dict=attribs, group_name=group_name)

class MDFEvalNode(MDFNode):

    _staticmethod_counter = itertools.count()

    def __init__(self, func, name=None, short_name=None, fqname=None, cls=None, category=None, filter=None):
        self._func = self._validate_func(func)
        self._bound_nodes = {}
        self._is_generator = _isgeneratorfunction(self._func)
        self._filter_func = filter
        if name is None:
            name = self._get_func_name(func)
        MDFNode.__init__(self, name=name, short_name=short_name, fqname=fqname, cls=cls, category=category)
        self._has_timestep_update = self._is_generator
        
        # get func_doc first then __doc__ to allow instances (iterators etc) to set their own docstring
        self.func_doc = getattr(func, "func_doc", None)
        if self.func_doc is None:
            self.func_doc = getattr(func, "__doc__", None)

    @property
    def node_type(self):
        """returns the name of the node type of this node"""
        return "evalnode"

    @property
    def func(self):
        """
        user-level function that is the end function being
        called by the node - this is not necessarily the
        inner eval function as that may be some wrapper or modified
        function in the case of custom nodes (eg queuenodes)
        """ 
        return self._func

    @property
    def func_name(self):
        return self._get_func_name(self.func)

    def _set_func(self, func):
        """
        sets the function for this evalnode - only to be used by sub-classes
        that need to re-set the function after construction.
        """
        self._func = func
        self._is_generator = _isgeneratorfunction(self._func)

        # update the docstring
        self.func_doc = getattr(func, "func_doc", None)
        if self.func_doc is None:
            self.func_doc = getattr(func, "__doc__", None)

    def _bind(self, other, owner):
        """
        bind is called when the node is 'got' for a class instance.
        A new node is created, and this is called on the new node
        with the original node and the owning class to which this
        new node is to be bound.
        """
        if isinstance(other._filter_func, (types.FunctionType, MDFEvalNode)) and _is_member_of(owner, other._filter_func):
            if isinstance(other._filter_func, MDFEvalNode):
                self._filter_func = other._filter_func.__get__(None, owner)
            else:
                self._filter_func = self._bind_function(other._filter_func, owner)
        else:
            self._filter_func = other._filter_func

        # set the docstring for the bound node to the same as the unbound one
        self.func_doc = other.func_doc

    def _bind_function(self, func, owner):
        """convenience method for binding a function to an owner"""
        if owner is None:
            return func

        if isinstance(func, types.FunctionType):
            if func.__code__.co_argcount == 0:
                return staticmethod(func).__get__(None, owner)

            if func.__code__.co_argcount == 1:
                return classmethod(func).__get__(None, owner)

            raise Exception("Node functions aren't expected to have parameters")

        if isinstance(func, (classmethod, staticmethod, MDFCallable)):
            return func.__get__(None, owner)

        if isinstance(func, (types.MethodType,
                             MDFIteratorFactory,
                             types.BuiltinFunctionType,
                             MethodWrapperType)):
            return func

        if isinstance(func, types.TypeType) and issubclass(func, MDFIterator):
            return MDFIteratorFactory(func, owner)

        raise Exception("MDFEvalNode._bind_function called with unexpected type %s" % type(func))

    # MDFEvalNode is also a descriptor and can be bound to classes
    # to produce new nodes with the target function bound to the class
    def __get__(self, instance, owner):
        assert instance is None, "evalnodes can only be accessed via the class, not instance"

        # if a node's been bound to a class or an instance don't re-bind it
        node = cython.declare(MDFEvalNode)
        node = self
        if node._is_bound:
            return self

        node = self._bound_nodes.get(owner, None)
        if node is None:
            # look for the class this node applies to as owner may be a subclass
            base_cls = owner
            for cls in owner.mro():
                # this slightly cumbersome check is to avoid testing equality with any objects
                # that don't return bools when compared (eg pandas Series and DataFrames)
                if self in [x for x in cls.__dict__.values() if isinstance(x, self.__class__)]:
                    base_cls = cls
                    break

            # get the bound function
            func = self._bind_function(self._func, owner)

            # create the new node and bind it to the owner
            node = self.__class__(func, name=self.func_name, cls=(owner, base_cls), category=self.categories)
            node._bind(self, owner)

            # cache it for next time
            self._bound_nodes[owner] = node
        return node

    def _get_func_name(self, func):
        """return the node name from a function"""
        if isinstance(func, (staticmethod, classmethod)):
            # staticmethods and classmethods don't have names until they're bound to a class
            return "%s.%d" % (func.__class__.__name__, next(self._staticmethod_counter))
        return _get_func_name(func)

    def _validate_func(self, func):
        assert not isinstance(func, MDFNode), "evalnodes can't wrap other nodes"
        assert callable(func), "evalnodes must be callable"
        return func

    def get_filter(self):
        return self._filter_func

    def has_timestep_update(self, ctx):
        """returns True if the node value can be updated incrementally as time is updated"""
        return self._is_generator

    def _get_value(self, ctx, node_state):
        # if there's a timestep func and nothing's changed apart from the
        # date look for a previous value and call the timestep func
        dirty_flags = node_state.dirty_flags
        if self._is_generator \
        and dirty_flags == DIRTY_FLAGS_TIME \
        and node_state.generator is not None: 
            # if this node has been valued already for this context
            # check the date and see if it can be updated from that
            try:
                # don't unpack as a tuple as the produces slighly more complicated cython code
                tmp = self._get_cached_value_and_date(ctx, node_state)
                prev_value = tmp[0]
                prev_date = tmp[1]
            except KeyError:
                prev_value, prev_date = None, None

            if prev_date is not None:
                date_cmp = cython.declare(int)
                date_cmp = 0 if prev_date == ctx._now else (-1 if prev_date < ctx._now else 1)
                if date_cmp == 0: # prev_date == ctx._now
                    self._touch(node_state, DIRTY_FLAGS_ALL, True)
                    return prev_value

                if date_cmp < 0: # prev_date < ctx._now
                    # if a filter's set check if the previous value can be re-used
                    if self._filter_func is not None:
                        if _profiling_enabled:
                            with ctx._profile(self) as timer:
                                needs_update = self._filter_func()
                        else:
                            needs_update = self._filter_func()

                        if not needs_update:
                            # re-use the previous value
                            if _trace_enabled:
                                _logger.debug("Re-using previous value of %s[%s]" % (self.name, ctx))

                            self._touch(node_state, DIRTY_FLAGS_ALL, True)
                            return prev_value

                    # call the timestep function with or without the context
                    if _trace_enabled:
                        _logger.debug("Evaluating next value of %s[%s]" % (self.name, ctx))

                    if _profiling_enabled:
                        with ctx._profile(self):
                            new_value = next(node_state.generator)
                    else:
                        new_value = next(node_state.generator)

                    return new_value

        # if still dirty call func to get the new value
        if _trace_enabled:
            _logger.debug("Evaluating %s[%s] (%s)" % (self.name,
                                                      ctx,
                                                      DIRTY_FLAGS.to_string(dirty_flags)))

        # call the function, set the value and return it
        if _profiling_enabled:
            with ctx._profile(self) as timer:
                value = self._func()
        else:
            value = self._func()

        if self._is_generator:
            gen = value

            # evaluate the generator
            if _profiling_enabled:
                with ctx._profile(self) as timer:
                    value = next(gen)
            else:
                value = next(gen)

            node_state.generator = iter(gen)

            # if a filter's set call it just to make sure any dependencies it requires
            # are set up correctly to avoid conditional dependency errors later
            if self._filter_func is not None:
                if _profiling_enabled:
                    with ctx._profile(self) as timer:
                        self._filter_func()
                else:
                    self._filter_func()

        return value

    def _get_alt_context(self, ctx):
        """
        returns the context values for this node[ctx] actually
        belong in. This can be different from ctx if its a
        shifted context but this node doesn't depend on that shift.
        """
        ctx_shift_set = ctx.get_shift_set()

        # if the context hasn't been shifted then the context to use
        # is trivially this context
        if not ctx_shift_set:
            return ctx

        # if the node's been overriden get the alt context from the override
        node_state = self._get_state(ctx)
        override_node = self._get_override(ctx, node_state)
        if override_node and override_node is not self:
            override_alt_ctx = cython.declare(MDFContext)
            override_alt_ctx = override_node.get_alt_context(ctx)
            return override_alt_ctx.shift({self : override_node})

        # cython forward declarations
        parent = cython.declare(MDFContext)
        shifted_ctx = cython.declare(MDFContext)
        best_match = cython.declare(MDFContext)
        shifted_node_state = cython.declare(NodeState)
        num_shifts = cython.declare(int)
        best_match_num_shifts = cython.declare(int)
        shifted_node = cython.declare(MDFNode)

        # find the most shifted context where this node has been called
        # and where this context is a shift of that shifted context
        parent = ctx.get_parent() or ctx

        best_match = None
        best_match_num_shifts = -1
        for shifted_ctx in itertools.chain([parent], parent.iter_shifted_contexts()):
            try:
                shifted_node_state = self._states[shifted_ctx._id_obj]
            except KeyError:
                continue

            if shifted_node_state.called:
                if shifted_ctx is ctx or ctx.is_shift_of(shifted_ctx):
                    # ctx is a shift of shifted_ctx so use the dependencies
                    # from this context to determine the correct alt context
                    num_shifts = len(shifted_ctx.get_shift_set())
                    if num_shifts > best_match_num_shifts:
                        best_match = shifted_ctx
                        best_match_num_shifts = num_shifts

                        # early out if this context is the main context
                        # since there can't be a candidate that ctx is a
                        # shift of and is more shifted
                        if shifted_ctx is ctx:
                            break

        # if it's never been called before in any related context, use this context
        if best_match is None:
            return ctx

        # get all the contexts that are shifts of the original context (or are the ctx)
        # or that original context is a shift of. i.e. it's a list of all contexts from the root
        # to ctx plus any shifts of ctx.
        #
        # NOTE this is *not* all the shifts and parents of best_match as that could be a much
        # wider set and could include contexts that are actually not shifts or parents of
        # the original context we're trying to get the value in.
        all_shifted_ctxs = cqueue()
        cqueue_push(all_shifted_ctxs, parent)
        for shifted_ctx in parent.iter_shifted_contexts():
            if shifted_ctx is ctx \
            or shifted_ctx.is_shift_of(ctx) \
            or ctx.is_shift_of(shifted_ctx):
                cqueue_push(all_shifted_ctxs, shifted_ctx)

        # sort so the least shifted are at the start. This should be more optimal
        # for the next loop than if they were in a random order.
        def get_shift_degree(x):
            return len(x.get_shift_set())
        cqueue_sort(all_shifted_ctxs, get_shift_degree, False)

        best_match_state = cython.declare(NodeState)
        best_match_state = self._get_state(best_match)

        shift_set = cython.declare(dict)
        shift_set = {}
        for shifted_node, shifted_value in ctx_shift_set.iteritems():
            # if the context is shifted by self then include self in the shift set
            if shifted_node is self:
                shift_set[shifted_node] = shifted_value
                continue

            # if self[best_match] depends on shifted_node[best_match] or
            # on shifted_node[shifted_ctx] where shifted_ctx is any shift
            # of the original ctx then we have to include the shifted node in the
            # shift set
            for shifted_ctx in all_shifted_ctxs:
                if self._depends_on(best_match_state, shifted_node, shifted_ctx._id_obj):
                    shift_set[shifted_node] = shifted_value
                    break

        return parent.shift(shift_set)

    def _fixup_alt_context(self, ctx, node_state, alt_ctx):
        # make self[alt_ctx] dependent on the same things as self[ctx]
        callee_ctx = cython.declare(MDFContext)
        alt_state = cython.declare(NodeState)
        alt_state = self._get_state(alt_ctx)
        for ctx_id, callees in node_state.callees.iteritems():
            callee_ctx = _get_context(ctx_id)
            for callee in callees:
                self.add_dependency(alt_ctx, callee, callee_ctx)

        # remove dependencies from self[ctx] since effectively we
        # called self[alt_ctx] instead
        node_state.callees.clear()
        self._clear_dependency_cache(ctx)

        # transfer the generator from ctx to alt_ctx
        alt_state.generator = node_state.generator
        node_state.generator = None

        node_state.called = False
        alt_state.called = True

    def _set_value(self, ctx, node_state, value, _quiet=True):
        # if this is the first time this node has been called
        # make sure the alt context gets re-calculated
        if not node_state.called:
            node_state.alt_context = None
            node_state.called = True

            # add an explicit dependnecy on now if incrementally updated
            if self._has_timestep_update:
                now_ = cython.declare(MDFTimeNode)
                now_ = now
                self.add_dependency(ctx, now_, now_.get_alt_context(ctx))

        alt_ctx = self.get_alt_context(ctx)
        alt_state = self._get_state(alt_ctx)
        MDFNode._set_value(self, alt_ctx, alt_state, value, _quiet)

        if alt_ctx is not ctx:
            self._fixup_alt_context(ctx, node_state, alt_ctx)

    def set_value(self, ctx, value):
        """
        Setting the value of an eval node fixes it to that value
        and clears any dependencies the node may have had.
        """
        # clear any dependencies this node has in this context
        node_state = self._get_state(ctx)
        node_state.callees.clear()
        self._clear_dependency_cache(ctx)

        # set the value
        MDFNode.set_value(self, ctx, value)

# for using decorator syntax to delclare eval nodes
def evalnode(func=None, filter=None, category=None):
    """
    Decorator for creating an :py:class:`MDFNode` whose value is determined
    by calling the function func.

    **func** should be a function or generator that takes no arguments and
    returns or yields the current value of the node.

    If *func* is a generator instead of a function it will be advanced as
    the :py:func:`now` node is advanced. This can be used to calculate
    accumulated values and maintain internal state over evaluations::

        @evalnode
        def some_function():
            # Set initial value of 'accum' to 0
            accum = 0

            # yield the initial value of 'accum' (0 in this case)
            yield accum

            while True:
                accum += 1
                # yield the updated value of 'accum' on each evaluation
                yield accum

    Yield essentially bookmarks the current execution point, and returns
    the supplied value (accum in the above example). When the node is
    evaluated again, execution will resume at the bookmark and continue
    until the next yield statement is encountered. By using an infinite
    while loop the node can be evaluated any number of times.

    The above example can actually be shortened to::

        @evalnode
        def some_function():
            # Set initial value of 'accum' to 0
            accum = 0

            while True:
                yield accum
                accum += 1

    In this case, yield will first return the initial value of 'accum'.
    On subsequent evaluations, the incrementation step will be also
    executed and the node will produce the updated value.

    **filter** may be used in the case when *func* is a generator to prevent
    the node valuation being advanced on every timestep. If supplied, it
    should be a function or node that returns True if the node should
    be advanced for the current timestep or False otherwise.
    """
    if func:
        return MDFEvalNode(func, category=category, filter=filter)
    return lambda x: evalnode(x, filter, category)

class MDFTimeNode(MDFVarNode):

    def __init__(self, name=None, cls=None, fqname=None, modulename=None):
        MDFVarNode.__init__(self, name, cls=cls, fqname=fqname)
        if modulename is not None:
            self._modulename = modulename

    @property
    def node_type(self):
        """returns the name of the node type of this node"""
        return "now"

    def _touch(self, node_state, flags=DIRTY_FLAGS_ALL, _quiet=False, _depth=0):
        # only set the TIME flag on dependent nodes
        MDFVarNode._touch(self, node_state, flags & DIRTY_FLAGS_TIME, _quiet, _depth)
        # but clear all flags on this node
        node_state.dirty_flags &= ~flags

    def set_value(self, ctx, value):
        # update the context's date to the new value
        if ctx._now != value:
            ctx.set_date(value)

        return MDFVarNode.set_value(self, ctx, value)

# there's one global 'now' node that gets the value of 'now' for each context
_now_node = MDFTimeNode(fqname="now", modulename="mdf")
now = _now_node

# Used to implement the reversed division binary operator
_one_node = varnode(name="__one__", default=1.0)