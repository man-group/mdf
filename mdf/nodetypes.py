from collections import deque, namedtuple
import operator
import datetime
import numpy as np
import pandas as pa
import inspect
import types
import sys
import cython

from .nodes import (
    MDFNode,
    MDFEvalNode,
    MDFIterator,
    MDFIteratorFactory,
    MDFCallable,
    _isgeneratorfunction,
    _is_member_of,
    _get_func_name,
    now,
)
from .context import MDFContext, _get_current_context
from .ctx_pickle import _unpickle_custom_node, _pickle_custom_node
from .parser import get_assigned_node_name
from .common import DIRTY_FLAGS

_python_version = cython.declare(int, sys.version_info[0])

@cython.cfunc
def _dict_iteritems(d):
    if _python_version > 2:
        return iter(d.items())
    return d.iteritems()


# strings are used as dict lookups using nans and np.float64 can be problematic
_special_floats = cython.declare(dict, {
    str(np.nan): "1.#QNAN",
    str(np.inf): "1.#INF",
    str(-np.inf): "-1.#INF"
})


class MDFCustomNodeIteratorFactory(MDFIteratorFactory):

    def __init__(self, custom_node):
        self.custom_node = custom_node

    @property
    def func(self):
        return self.custom_node._custom_iterator_func

    @property
    def func_doc(self):
        func_doc = getattr(self.func, "func_doc", None)
        if func_doc is None:
            func_doc = getattr(self.func, "__doc__", None)
        return func_doc

    @property
    def node_type_func(self):
        return self.custom_node._custom_iterator_node_type_func

    def __call__(self):
        return MDFCustomNodeIterator(self.custom_node)

class MDFCustomNodeIterator(MDFIterator):
    
    def __init__(self, custom_node):
        self.custom_node = custom_node

        self.func = custom_node._custom_iterator_func
        self.node_type_func = self.custom_node._custom_iterator_node_type_func

        self.value_generator = None
        self.is_generator = _isgeneratorfunction(self.func)

        self.node_type_is_generator = _isgeneratorfunction(self.node_type_func)
        self.node_type_generator = None

    def __iter__(self):
        return self

    def next(self):
        if self.custom_node._call_with_no_value:
            value = None
        else:
            if self.is_generator:
                if not self.value_generator:
                    self.value_generator = self.func()
                value = next(self.value_generator)
            else:
                value = self.func()

        if self.node_type_is_generator:
            if not self.node_type_generator:
                # create the new node type generator and return
                kwargs = self.custom_node._get_kwargs()
                self.node_type_generator = self.node_type_func(value, **kwargs)
                return next(self.node_type_generator)

            return self.node_type_generator.send(value)

        # node type is plain function
        kwargs = self.custom_node._get_kwargs()
        return self.custom_node._custom_iterator_node_type_func(value, **kwargs)

class MDFCustomNode(MDFEvalNode):
    """
    subclass of MDFEvalNode that forms the base for all over custom
    node types.
    """
    # override this in a subclass if any kwargs should be passed as nodes
    # instead of being evaluated
    nodetype_node_kwargs = set()
    
    # override this is the function being wrapped by the subclass can't
    # be inspected (eg is a cython function) and it takes some keyword
    # arguments.
    nodetype_kwargs = None

    # if set to True on the subclass the first parameter passed to the node
    # type function will always be none and the underlying node won't be
    # evaluated.
    call_with_no_value = False

    def __init__(self,
                    func,
                    node_type_func=None,
                    name=None,
                    short_name=None,
                    fqname=None,
                    cls=None,
                    category=None,
                    filter=None,
                    base_node=None, # set if created via MDFCustomNodeMethod
                    base_node_method_name=None,
                    nodetype_func_kwargs={}):
        if isinstance(func, MDFCustomNodeIteratorFactory):
            node_type_func = func.node_type_func
            func = func.func
        self._base_node = base_node
        self._base_node_method_name = base_node_method_name
        self._node_type_func = node_type_func
        self._cn_func = self._validate_func(func)
        self._category = category
        self._kwargs = dict(nodetype_func_kwargs)
        self._kwnodes = dict([(k, v) for (k, v) in _dict_iteritems(nodetype_func_kwargs) if isinstance(v, MDFNode)])
        self._kwfuncs = {} # reserved for functions added via decorators

        # if 'filter_node_value' is in the node type generator args we pass in the value of the filter
        kwargs = self._get_nodetype_func_kwargs(False)
        self._call_with_filter_node = "filter_node" in kwargs
        self._call_with_filter = "filter_node_value" in kwargs
        self._call_with_self = "owner_node" in kwargs
        self._call_with_no_value = self.call_with_no_value

        eval_func = self._cn_eval_func
        if _isgeneratorfunction(node_type_func) or _isgeneratorfunction(func):
            eval_func = MDFCustomNodeIteratorFactory(self)

        MDFEvalNode.__init__(self,
                             eval_func,
                             name=name or self._get_func_name(func),
                             short_name=short_name,
                             fqname=fqname,
                             cls=cls,
                             category=category,
                             filter=filter)

        # set func_doc from the inner function's docstring
        self.func_doc = getattr(func, "func_doc", None)
        if self.func_doc is None:
            self.func_doc = getattr(func, "__doc__", None)

    def __reduce__(self):
        """support for pickling"""
        kwargs = dict(self._kwargs)

        # add filter and category to the kwargs
        filter = self.get_filter()
        if filter is not None:
            kwargs["filter"] = filter
        if self._category is not None:
            kwargs["category"] = self._category

        return (
            _unpickle_custom_node,
            _pickle_custom_node(self, self._base_node, self._base_node_method_name, kwargs),
            None,
            None,
            None,
        )

    def _get_nodetype_func_kwargs(self, remove_special=True):
        """return a list of named arguments for the node type function"""
        kwargs = self.nodetype_kwargs

        if kwargs is None:
            # try and get them from the func/iterator object
            node_type_func = self._node_type_func
            argspec = None
            try:
                kwargs = node_type_func._init_kwargs_
            except AttributeError:
                init_kwargs = None

        if kwargs is None:
            # try and get them from the func/iterator object
            if isinstance(node_type_func, types.TypeType):
                node_type_func = node_type_func.__init__
            try:
                argspec = inspect.getargspec(node_type_func).args
            except TypeError:
                return []
            kwargs = argspec[1:]

        # remove 'special' kwargs
        if remove_special:
            for special in ("filter_node", "filter_node_value", "owner_node"):
                if special in kwargs:
                    kwargs = list(kwargs)
                    kwargs.remove(special)

        return kwargs

    def __getattr__(self, attr):
        # give the superclass a go first
        try:
            # super doesn't work well with cython
            return MDFEvalNode.__getattr__(self, attr)
        except AttributeError:
            pass

        # return a decorator function for setting kwargs for the inner function
        # if attr is in the argspec for the node function
        if attr.startswith("_") or self._node_type_func is None:
            raise AttributeError(attr)

        kwargs = self._get_nodetype_func_kwargs()
        if attr not in kwargs:
            raise AttributeError(attr)

        def _make_decorator(attr):
            # the decorator takes either a value, function or node
            # do you can do things like:
            #
            # @mynodetype
            # def func():
            #    ...
            #
            # @func.some_kwarg
            # def kwarg_value():
            #    ...
            # 
            def _decorator(func):
                self._kwargs[attr] = func
                if isinstance(func, MDFNode):
                    self._kwnodes[attr] = func
                elif isinstance(func, types.FunctionType):
                    self._kwfuncs[attr] = func
                return func
            return _decorator

        return _make_decorator(attr)

    @property
    def node_type(self):
        """returns the name of the node type of this node"""
        try:
            return _get_func_name(self._node_type_func)
        except AttributeError:
            return "customnode"

    @property
    def func(self):
        return self._cn_func


    @property
    def base_node(self):
        """node this custom node was derived from if created via a method call."""
        return self._base_node

    #
    # Properties for use with MDFCustomNodeIterator
    #
    # The iterator uses these instead of being constructed with them
    # as it needs to be pickleable, and so by keeping a reference
    # to the node that can be unpickled more easily than a function.
    #
    @property
    def _custom_iterator_node_type_func(self):
        return self._node_type_func

    @property
    def _custom_iterator_func(self):
        return self._cn_func

    def _set_func(self, func):
        # set the underlying MDFEvalNode func
        if _isgeneratorfunction(func) or _isgeneratorfunction(self._node_type_func):
            MDFEvalNode._set_func(self, MDFCustomNodeIteratorFactory(self))
        else:
            MDFEvalNode._set_func(self, self._cn_eval_func)

        # update the docstring
        self.func_doc = getattr(func, "func_doc", None)
        if self.func_doc is None:
            self.func_doc = getattr(func, "__doc__", None)

        # set the func used by this class
        self._cn_func = func

    def _bind(self, other_evalnode, owner):
        other = cython.declare(MDFCustomNode)
        other = other_evalnode
        MDFEvalNode._bind(self, other, owner)
        self._node_type_func = other._node_type_func
        func = self._bind_function(other._cn_func, owner)
        self._set_func(func)

        # copy the kwargs
        self._kwargs = dict(other._kwargs)
        
        # bind the eval nodes (won't do anything if they're already bound)
        self._kwnodes = {}
        for k, node in _dict_iteritems(other._kwnodes):
            if isinstance(node, MDFEvalNode) and _is_member_of(owner, node):
                self._kwnodes[k] = node.__get__(None, owner)
            else:
                self._kwnodes[k] = node

        # bind the functions in case they're classmethods
        self._kwfuncs = {}
        for k, func in _dict_iteritems(other._kwfuncs):
            if _is_member_of(owner, func):
                self._kwfuncs[k] = self._bind_function(func, owner)

        # set the docstring for the bound node to the same as the unbound one
        self.func_doc = other.func_doc

    def _get_kwargs(self):
        kwargs = cython.declare(dict)
        kwargs = self._kwargs
        if self._kwnodes or self._kwfuncs:
            kwargs = dict(kwargs)

            node = cython.declare(MDFNode)
            for key, node in _dict_iteritems(self._kwnodes):
                if key not in self.nodetype_node_kwargs:
                    kwargs[key] = node()
                else:
                    kwargs[key] = node

            for key, value in _dict_iteritems(self._kwfuncs):
                kwargs[key] = value()

        # if the filter value should be passed in as a kwarg
        # add it in now (defaulting to True)
        if self._call_with_filter:
            filter_node = self.get_filter()
            filter_node_value = True
            if filter_node is not None:
                filter_node_value = filter_node()
            kwargs["filter_node_value"] = filter_node_value

        if self._call_with_filter_node:
            kwargs["filter_node"] = self.get_filter()

        if self._call_with_self:
            kwargs["owner_node"] = self

        return kwargs

    def _cn_eval_func(self):
        # get the inner node value
        if self._call_with_no_value:
            value = None
        else:
            value = self._cn_func()

        # and call the node type function
        kwargs = cython.declare(dict)
        kwargs = self._get_kwargs()
        return self._node_type_func(value, **kwargs)

class MDFCustomNodeMethod(object):
    """
    Callable object that is added to MDFNode's set of attributes
    to work as an additional method for calling node types
    directly from a node rather than explicitly creating
    derived nodes.
    
    eg: instead of:

    @delaynode(periods=10)
    def my_delayed_node():
        return my_node()
        

    it is possible to simply call:
    
    my_node.delay(periods=10)

    """
    # this class behaves like a method, but types.MethodType isn't subclass-able

    def __init__(self,
                 node_type_func,
                 node_cls,
                 method_name,
                 node=None,
                 call=True): # if call is True __call__ will call the derived node
        self._node_type_func = node_type_func
        self._node_cls = node_cls
        self._method_name = method_name
        self._node = node
        self._call = call
        self._derived_nodes = node._derived_nodes if node else {}

    def __get__(self, instance, cls=None):
        if instance and self._node is instance:
            return self

        return MDFCustomNodeMethod(self._node_type_func,
                                   self._node_cls,
                                   self._method_name,
                                   node=instance,
                                   call=self._call)

    def __repr__(self):
        # try to get the args directly from the iterator (if it is one)
        args = self._node_cls.nodetype_kwargs
        if args is None:
            try:
                args = self._node_type_func._init_kwargs_
            except AttributeError:
                args = None

        if args is None:
            # otherwise try and use inspect to get the args, but this won't
            # work for cythoned functions
            try:
                if isinstance(self._node_type_func, types.TypeType):
                    args = inspect.getargspec(self._node_type_func.__init__).args[2:]
                else:
                    args = inspect.getargspec(self._node_type_func).args[1:]
            except TypeError:
                args = ["..."]

        args = ", ".join(args)

        if self._node:
            return "<MDFCustomNodeMethod %s.%s(%s)>" % (self._node.name,
                                                         self._method_name,
                                                         args)
        return "<unbound MDFCustomNodeMethod %s(%s)>" % (self._method_name, args)                                                         

    def __call__(self,
                    name=None,
                    short_name=None,
                    filter=None,
                    category=None,
                    **kwargs):
        # get the derived node and call it
        derived_node = self._get_derived_node(name=name,
                                              short_name=short_name,
                                              filter=filter,
                                              category=category,
                                              nodetype_func_kwargs=kwargs)
        if self._call:
            return derived_node()
        return derived_node

    def _get_derived_node(self,
                            name=None,
                            short_name=None,
                            filter=None,
                            category=None,
                            nodetype_func_kwargs={}):
        """
        return a new or cached node made from the base node with
        the node type func applied
        """
        # find the derived node for these arguments
        derived_node_key = cython.declare(tuple)
        derived_node = cython.declare(MDFEvalNode)

        # replace any special floats with string versions so they compare correctly
        # when looking for an existing node
        kwargs_in_key = []
        for key, value in _dict_iteritems(nodetype_func_kwargs):
            if value != value:
                try:
                    value = _special_floats[str(value)]
                except KeyError:
                    value = "1.#%s" % value
            kwargs_in_key.append((key, value))

        derived_node_key = (self._node_type_func,
                            self._node_cls,
                            filter,
                            category,
                            frozenset(kwargs_in_key))
        try:
            derived_node = self._derived_nodes[derived_node_key]
        except KeyError:
            # use name and short_name if present. If name is present but short_name
            # isn't, use name for short_name too.
            if short_name is None and name is not None:
                short_name = name

            # get all kwargs for the node func and the others passed to this func
            # to build the node name
            if name is None:
                kwargs = dict(nodetype_func_kwargs)
                if filter is not None:
                    kwargs["filter"] = filter

                kwarg_strs = [None] * len(kwargs)
                short_kwarg_strs = [None] * len(kwargs)
                for i, (k, v) in enumerate(sorted(kwargs.items())):
                    vs = v
                    if isinstance(v, MDFNode):
                        vs = v.short_name
                        v = v.name
                    kwarg_strs[i] = "%s=%s" % (k, v)
                    short_kwarg_strs[i] = "%s=%s" % (k, vs)
    
                args = ", ".join(kwarg_strs)
                name = "%s.%s(%s)" % (self._node.name, self._method_name, args)

                if short_name is None:
                    short_args = ", ".join(short_kwarg_strs)
                    short_name = "%s.%s(%s)" % (self._node.short_name,
                                                self._method_name,
                                                short_args)

            derived_node = self._node_cls(self._node.__call__,
                                          self._node_type_func,
                                          name=name,
                                          short_name=short_name,
                                          fqname=name,
                                          category=category,
                                          base_node=self._node,
                                          base_node_method_name=self._method_name,
                                          filter=filter,
                                          nodetype_func_kwargs=nodetype_func_kwargs)

            # update the docstring
            derived_node.func_doc = "\n".join(("*Derived Node* ::", "",
                                                "    " + short_name, "",
                                                derived_node.func_doc or "")).strip()

            self._derived_nodes[derived_node_key] = derived_node

        return derived_node

class MDFCustomNodeDecorator(object):
    """
    decorator that applies a custom node type to a function to
    create a node.
    """
    def __init__(self,
                 node_type_func,
                 node_type_cls,
                 name=None,
                 short_name=None,
                 filter=None,
                 category=None,
                 kwargs={}):
        """
        functor type object that can be used as a decorator to create an
        instance of 'node_type_cls' with 'node_type_func'
        """
        self.func = node_type_func
        self.__node_type_cls = node_type_cls
        self.__filter = filter
        self.__name = name
        self.__short_name = short_name
        self._category = category
        self._kwargs = dict(kwargs)

        # set the docs for this object to the same as the underlying function
        if hasattr(node_type_func, "__doc__"):
            self.__doc__ = node_type_func.__doc__

    def __call__(self,
                    _func=None,
                    name=None,
                    short_name=None,
                    filter=None,
                    category=None,
                    **kwargs):
        """
        If func is None return a copy of self with category, filter
        and kwargs bound to what's passed in.

        Otherwise if func is not None decorate func with the node type.
        """
        filter = filter or self.__filter
        category = category or self._category
        kwargs = kwargs or self._kwargs

        if _func is None:
            return MDFCustomNodeDecorator(self.func,
                                          self.__node_type_cls,
                                          name,
                                          short_name,
                                          filter,
                                          category,
                                          kwargs)

        node = self.__node_type_cls(_func,
                                    self.func,
                                    name=name,
                                    short_name=short_name,
                                    category=category,
                                    filter=filter,
                                    nodetype_func_kwargs=kwargs)
        return node

def nodetype(func=None, cls=MDFCustomNode, method=None):
    """
    decorator for creating a custom node type::

        #
        # create a new node type 'new_node_type'
        #
        @nodetype
        def new_node_type(value, fast, slow):
            return (value + fast) * slow

        #
        # use the new type to create a node
        #
        @new_node_type(fast=1, slow=10)
        def my_node():
            return some_value

        # ctx[my_node] returns new_node_type(value=my_node(), fast=1, slow=10)

    The node type function takes the value of the decorated node
    and any other keyword arguments that may be supplied when
    the node is created.

    The node type function may be a plain function, in which case
    it is simply called for every evaluation of the node, or it
    may be a co-routine in which case it is sent the new value
    for each iteration::

        @nodetype
        def nansumnode(value):
            accum = 0.
            while True:
                accum = np.nansum([value, accum])
                value = yield accum

        @nansumnode
        def my_nansum_node():
            return some_value

    The kwargs passed to the node decorator may be values (as shown above)
    or nodes which will be evaluated before the node type function is
    called.

    Nodes defined using the @nodetype decorator may be applied to 
    classmethods as well as functions and also support the standard
    node kwargs 'filter' and 'category'.

    Node types may also be used to add methods to the MDFNode class
    (See :ref:`nodetype_method_syntax`)::

        @nodetype(method="my_nodetype_method")
        def my_nodetype(value, scale=1):
            return value * scale
    
        @evalnode
        def x():
            return ...
    
        @my_nodetype(scale=10)
        def y():
            return x()

        # can be re-written as:
        y = x.my_nodetype_method(scale=10)
    """
    if func is None:
        return lambda func: nodetype(func, cls, method)

    # set a new method on MDFNode if required
    if method is not None:
        # this method gets the node and evaluates it
        method_func = MDFCustomNodeMethod(func, cls, method, call=True)
        MDFNode._additional_attrs_[method] = method_func

        # add another method to access the node
        getnode_func = MDFCustomNodeMethod(func, cls, method, call=False)
        MDFNode._additional_attrs_[method + "node"] = getnode_func

    return MDFCustomNodeDecorator(func, cls)

class MDFQueueNode(MDFCustomNode):
    pass

class _queuenode(MDFIterator):
    """
    Decorator for creating an :py:class:`MDFNode` that accumulates
    values in a `collections.deque` each time the context's date
    is advanced.

    The values that are accumulated are the results of the function
    `func`. `func` is a node function and takes no arguments.

    If `size` is specified the queue will grow to a maximum of that size
    and then values will be dropped off the queue (FIFO).

    `size` may either be a value or a callable (i.e. a function or a node)::

        @queuenode(size=10)
        def node():
            return x

    or::

        # could be an evalnode also
        queue_size = varnode("queue_size", 10)

        @queunode(size=queue_size)
        def node():
            return x

    or using the nodetype method syntax (see :ref:`nodetype_method_syntax`)::

        @evalnode
        def some_value():
            return ...

        @evalnode
        def node():
            return some_value.queue(size=5)
    """
    _init_kwargs_ = ["filter_node_value", "size", "as_list"]

    def __init__(self, value, filter_node_value, size=None, as_list=False):
        if size is not None:
            size = max(size, 1)

        self.as_list = as_list

        # create the queue used for the queue data
        self.queue = deque([], size)
        
        # only include the current value if the filter is
        # True (or if there's no filter being applied)
        if filter_node_value:
            self.queue.append(value)

    def next(self):
        if self.as_list:
            return list(self.queue)
        return self.queue

    def send(self, value):
        self.queue.append(value)
        if self.as_list:
            return list(self.queue)
        return self.queue

# decorators don't work on cythoned classes
queuenode = nodetype(_queuenode, cls=MDFQueueNode, method="queue")

class MDFDelayNode(MDFCustomNode):
    """
    MDFDelayedNode is different from other MDFCustomNodes.
    The value passed to the node function is actually the
    value *from the previous day*.
    
    This breaks the recursion for nodes that want to access
    a delayed version of themself, eg::
    
        @delaynode(periods=1, initial_value=0, lazy=True)
        def delayed_foo():
            return foo()
            
        @evalnode
        def foo():
            return 1 + delayed_foo()
        
    Here calling delayed_foo doesn't causes a recursive call to
    foo since MDFDelayedNode doesn't call the function
    immediately, it waits until the timestep is about to be
    advanced.
    """
    PerCtxData = namedtuple("PerCtxData", ["value", "generator", "date", "is_valid"])

    def __init__(self,
                 func,
                 node_type_func=None,
                 name=None,
                 short_name=None,
                 fqname=None,
                 cls=None,
                 category=None,
                 filter=None,
                 nodetype_func_kwargs={},
                 **kwargs):
        if isinstance(func, MDFCustomNodeIteratorFactory):
            node_type_func = func.node_type_func
            func = func.func
        self._dn_func = func
        self._dn_per_ctx_data = {}
        self._dn_is_generator = _isgeneratorfunction(func)
        self._dn_lazy = nodetype_func_kwargs.get("lazy", False)

        #
        # the node is initialized either just with the plain node function
        # (if lazy is False), or with the _dn_get_prev_value method if
        # lazy is true. The previous value is evaluated during
        # MDFContext.set_date by the _on_set_date callback.
        #
        MDFCustomNode.__init__(self,
                                self._dn_get_prev_value if self._dn_lazy else func,
                                node_type_func,
                                name=name or self._get_func_name(func),
                                short_name=short_name,
                                fqname=fqname,
                                cls=cls,
                                category=category,
                                filter=filter,
                                nodetype_func_kwargs=nodetype_func_kwargs,
                                **kwargs)

    @property
    def func(self):
        return self._dn_func

    def clear_value(self, ctx):
        try:
            del self._dn_per_ctx_data[ctx._id]
        except KeyError:
            pass

        MDFCustomNode.clear_value(self, ctx)

    def clear(self, ctx):
        self.clear_value(ctx)
        MDFCustomNode.clear(self, ctx)

    def _bind(self, other_node, owner):
        other = cython.declare(MDFDelayNode)
        other = other_node
        MDFCustomNode._bind(self, other, owner)
        self._dn_func = self._bind_function(other._dn_func, owner)
        self._dn_is_generator = other._dn_is_generator
        self._dn_lazy = other._dn_lazy

        func = self._dn_get_prev_value if self._dn_lazy else self._dn_func
        self._set_func(func)

        # set the docstring for the bound node to the same as the unbound one
        self.func_doc = other.func_doc

    def _dn_get_prev_value(self):
        # The value returned on date 'now' is the value for the previous day. 
        # This means that the value for now doesn't have to be calculated
        # until just before now is advanced. This breaks the recursion
        # of functions that want to call a node that is a delayed version
        # of the same node.
        ctx = _get_current_context()
        alt_ctx = self.get_alt_context(ctx)
        try:
            data = self._dn_per_ctx_data[alt_ctx._id]
            if data.is_valid:
                return data.value
        except KeyError:
            pass

        kwargs = self._get_kwargs()
        return kwargs["initial_value"]

    def on_set_date(self, ctx_, date):
        """called just before 'now' is advanced"""
        ctx = cython.declare(MDFContext)
        ctx = ctx_

        # if not lazy there's nothing to do
        if not self._dn_lazy:
            return False

        # grab the original alt ctx before it's modified by calling the node func
        orig_alt_ctx = self.get_alt_context(ctx)

        if date > ctx._now:
            # if this node hasn't been valued before, clear any previous
            # alt context set as the dependencies wouldn't have been set
            # up when the alt ctx was determined
            if ctx._id not in self._dn_per_ctx_data: 
                self._reset_alt_context(ctx)

            # if there's a filter set don't update the previous value unless
            # the filter returns True
            filter = self.get_filter()
            if filter is not None and not filter():
                return False

            # if there's already a value for this date then don't do anything
            alt_ctx = self.get_alt_context(ctx)
            alt_data = self._dn_per_ctx_data.get(alt_ctx._id)
            if alt_data and alt_data.is_valid and alt_data.date == date:
                return False

            # get the current value of the node function/generator
            generator = None
            if self._dn_is_generator:
                if alt_data:
                    generator = alt_data.generator
                if not generator:
                    generator = self._dn_func()
                value = next(generator)
            else:
                value = self._dn_func()

            # get the alt context again as it could have changed because of new 
            # dependencies added when calling the node function/generator
            alt_ctx = self.get_alt_context(ctx)

            # store the generator and value in the alt_ctx, and put an invalid entry
            # in the original context so this context doesn't get its alt ctx reset next time
            self._dn_per_ctx_data[alt_ctx._id] = self.PerCtxData(value, generator, date, True)
            if alt_ctx is not ctx:
                self._dn_per_ctx_data[ctx._id] = self.PerCtxData(None, None, date, False)

        elif date < ctx._now:
            self.clear_value(ctx)
            alt_ctx = self.get_alt_context(ctx)
            if alt_ctx is not ctx:
                self.clear_value(alt_ctx)

        # return True to indicate the value of this node will change after the date has
        # finished being changed.
        return True

class _delaynode(MDFIterator):
    """
    Decorator for creating an :py:class:`MDFNode` that delays
    values for a number of periods corresponding to each time
    the context's date is advanced.

    The values that are delayed are the results of the function
    `func`. `func` is a node function and takes no arguments.

    ``periods`` is the number of timesteps to delay the value by.

    ``initial_value`` is the value of the node to be used before
    the specified number of periods have elapsed.

    `periods`, `initial_value` and `filter` can either be values
    or callable objects (e.g. a node or a function)::

        @delaynode(periods=5)
        def node():
            return x

    or::

        # could be an evalnode also
        periods = varnode("periods", 5)

        @delaynode(periods=periods)
        def node():
            return x
            
    If ``lazy`` is True the node value is calculated after any calling
    nodes have returned. This allows nodes to call delayed version of
    themselves without ending up in infinite recursion.
    
    The default for ``lazy`` is False as in most cases it's not
    necessary and can cause problems because the dependencies aren't
    all discovered when the node is first evaluated.
    
    e.g.::
    
        @delaynode(periods=10)
        def node():
            return some_value

    or using the nodetype method syntax (see :ref:`nodetype_method_syntax`)::

        @evalnode
        def some_value():
            return ...

        @evalnode
        def node():
            return some_value.delay(periods=5)
    """
    _init_kwargs_ = ["filter_node_value", "periods", "initial_value", "lazy", "ffill"]

    def __init__(self, value, filter_node_value, periods=1,
                    initial_value=None, lazy=False, ffill=False):
        self.lazy = lazy
        self.skip_nans = ffill
        max_queue_size = 0

        # if the initial value is a scalar but the value is a vector
        # broadcast the initial value
        if isinstance(initial_value, (int, float)):
            if isinstance(value, pa.Series):
                initial_value = pa.Series(initial_value, index=value.index, dtype=value.dtype)
            elif isinstance(value, np.ndarray):
                tmp = np.ndarray(value.shape, dtype=value.dtype)
                tmp.fill(initial_value)
                initial_value = tmp

        if lazy:
            # NOTE: when lazy the value is *already delayed by 1* (see MDFDelayNode)
            assert periods is not None and periods > 0, "lazy delay nodes must have 'periods' set to > 0"
            max_queue_size = periods


        else:
            # max size is periods+1 (if a value is delayed 0 periods the length of the queue must be 1)
            assert periods is not None and periods >= 0, "delay nodes must have 'periods' set to >= 0"
            max_queue_size = periods + 1

        # create the queue and fill it with the initial value
        self.queue = deque([initial_value] * max_queue_size, max_queue_size)

        # send the current value if the filter value is True, or if the node
        # is lazy. If it's lazy the filtering is done by the on_set_date callback
        # since it needs to be filtered based on the previous filter value.
        if filter_node_value or lazy:
            self.send(value)

    def next(self):
        return self.queue[0]

    def send(self, value):
        skip = False
        if self.skip_nans:
            if isinstance(value, float):
                if np.isnan(value):
                    skip = True
            elif isinstance(value, np.ndarray):
                if np.isnan(value).all():
                    skip = True
        if not skip:
            self.queue.append(value)
        return self.queue[0]

# decorators don't work on cythoned classes
delaynode = nodetype(_delaynode, cls=MDFDelayNode, method="delay")

class MDFSampleNode(MDFCustomNode):
    # always pass date_node as the node rather than evaluate it
    nodetype_node_kwargs = ["date_node"]

class _samplenode(MDFIterator):
    """
    samples value on the given date offset and yields that value
    until the next date offset.
    
    offset is a pandas.datetools.DateOffset instance,
    eg pandas.datetools.BMonthEnd()
    """    
    _init_kwargs_ = ["filter_node_value", "offset", "date_node", "initial_value"]

    def __init__(self, value, filter_node_value, offset, date_node=now, initial_value=None):
        self._offset = offset
        self._date_node = date_node

        # if the initial value is a scalar but the value is a vector
        # broadcast the initial value
        if isinstance(initial_value, (int, float)):
            if isinstance(value, pa.Series):
                initial_value = pa.Series(initial_value, index=value.index, dtype=value.dtype)
            elif isinstance(value, np.ndarray):
                tmp = np.ndarray(value.shape, dtype=value.dtype)
                tmp.fill(initial_value)
                initial_value = tmp

        self._sample = initial_value

        if filter_node_value:
            self.send(value)

    def next(self):
        return self._sample

    def send(self, value):
        date = self._date_node()
        if date is not None and self._offset.onOffset(date):
            self._sample = value 
        return self._sample

# decorators don't work on cythoned classes
samplenode = nodetype(_samplenode, cls=MDFSampleNode, method="sample")

class MDFNanSumNode(MDFCustomNode):
    pass

class _nansumnode(MDFIterator):
    """
    Decorator that creates an :py:class:`MDFNode` that maintains
    the `nansum` of the result of `func`.

    Each time the context's date is advanced the value of this
    node is calculated as the nansum of the previous value
    and the new value returned by `func`.
    
    e.g.::
    
        @nansumnode
        def node():
            return some_value

    or using the nodetype method syntax (see :ref:`nodetype_method_syntax`)::

        @evalnode
        def some_value():
            return ...

        @evalnode
        def node():
            return some_value.nansum()
    """
    _init_kwargs_ = ["filter_node_value"]

    def __init__(self, value, filter_node_value):
        self.is_float = False
        if isinstance(value, pa.Series):
            self.accum = pa.Series(np.nan, index=value.index, dtype=value.dtype)
        elif isinstance(value, np.ndarray):
            self.accum = np.ndarray(value.shape, dtype=value.dtype)
            self.accum.fill(np.nan)
        else:
            self.is_float = True
            self.accum_f = np.nan
            
        if filter_node_value:
            self.send(value)

    def _send_vector(self, value):
        mask = ~np.isnan(value)

        # set an nans in the accumulator where the value is not
        # NaN to zero
        accum_mask = np.isnan(self.accum)
        if accum_mask.any():
            self.accum[accum_mask & mask] = 0.0

        self.accum[mask] += value[mask]
        return self.accum.copy()

    def _send_float(self, value):
        if value == value:
            if self.accum_f != self.accum_f:
                self.accum_f = 0.0
            self.accum_f += value
        return self.accum_f

    def next(self):
        if self.is_float:
            return self.accum_f
        return self.accum.copy()

    def send(self, value):
        if self.is_float:
            return self._send_float(value)
        return self._send_vector(value)

# decorators don't work on cythoned types
nansumnode = nodetype(cls=MDFNanSumNode, method="nansum")(_nansumnode)

class MDFCumulativeProductNode(MDFCustomNode):
    pass

class _cumprodnode(MDFIterator):
    """
    Decorator that creates an :py:class:`MDFNode` that maintains
    the cumulative product of the result of `func`.

    Each time the context's date is advanced the value of this
    node is calculated as the previous value muliplied by
    the new value returned by `func`.
    
    e.g.::
    
        @cumprodnode
        def node():
            return some_value

    or using the nodetype method syntax (see :ref:`nodetype_method_syntax`)::

        @evalnode
        def some_value():
            return ...

        @evalnode
        def node():
            return some_value.cumprod()
            
    TODO: That node needs a test for the argument skipna, since it is not entirely clear what it should do if the first value is na.
    It would be nice to be able to specify an initial value.
    """
    _init_kwargs_ = ["filter_node_value", "skipna"]

    def __init__(self, value, filter_node_value, skipna=True):
        self.is_float = False
        self.skipna = skipna
        if isinstance(value, pa.Series):
            self.accum = pa.Series(np.nan, index=value.index, dtype=value.dtype)
            self.nan_mask = np.isnan(self.accum)
        elif isinstance(value, np.ndarray):
            self.accum = np.ndarray(value.shape, dtype=value.dtype)
            self.accum.fill(np.nan)
            self.nan_mask = np.isnan(self.accum)
        else:
            self.is_float = True
            self.accum_f = np.nan
            self.nan_mask_f = True

        if filter_node_value:
            self.send(value)

    def _send_vector(self, value):
        # we keep track of a nan mask rather than re-evalute it each time
        # because if accum became nan after starting we wouldn't want to
        # start it from 1 again
        if self.nan_mask.any():
            self.nan_mask = self.nan_mask & np.isnan(value)
            self.accum[~self.nan_mask & ~np.isnan(value)] = 1.0

        if self.skipna:
            mask = ~np.isnan(value)
            self.accum[mask] *= value[mask]
        else:
            self.accum *= value
        
        return self.accum.copy()

    def _send_float(self, value):
        if self.nan_mask_f:
            if value == value:
                self.nan_mask_f = False
                self.accum_f = 1.0

        if not self.skipna \
        or value == value:
            self.accum_f *= value

        return self.accum_f

    def next(self):
        if self.is_float:
            return self.accum_f
        return self.accum.copy()

    def send(self, value):
        if self.is_float:
            return self._send_float(value)
        return self._send_vector(value)

# decorators don't work on cythoned types
cumprodnode = nodetype(cls=MDFCumulativeProductNode, method="cumprod")(_cumprodnode)


class MDFForwardFillNode(MDFCustomNode):
    pass

class _ffillnode(MDFIterator):
    """
    Decorator that creates an :py:class:`MDFNode` that returns
    the current result of the decoratored function forward
    filled from the previous value where the current value
    is NaN.
    
    The decorated function may return a float, pandas Series
    or numpy array.
    
    e.g.::
    
        @ffillnode
        def node():
            return some_value

    or using the nodetype method syntax (see :ref:`nodetype_method_syntax`)::

        @evalnode
        def some_value():
            return ...

        @evalnode
        def node():
            return some_value.ffill()
    """
    _init_kwargs_ = ["filter_node_value", "initial_value"]
    
    def __init__(self, value, filter_node_value, initial_value=None):
        self.is_float = False
        if isinstance(value, float):
            #
            # floating point fill forward
            # 
            self.is_float = True
            self.current_value_f = initial_value if initial_value is not None else np.nan
        else:
            #
            # Series or ndarray fill forward
            #
            if not isinstance(value, (pa.Series, np.ndarray)):
                raise RuntimeError("fillnode expects a float, pa.Series or ndarray") 
    
            if initial_value is not None:
                if isinstance(initial_value, (float, int)):
                    if isinstance(value, pa.Series):
                        self.current_value = pa.Series(initial_value,
                                                       index=value.index,
                                                       dtype=value.dtype)
                    else:
                        self.current_value = np.ndarray(value.shape, dtype=value.dtype)
                        self.current_value.fill(initial_value) 
                else:
                    # this ensures the current_value ends up being the same type
                    # as value, even if initial_value is another vector type.
                    self.current_value = value.copy()
                    self.current_value[:] = initial_value[:]
            else:
                if isinstance(value, pa.Series):
                    self.current_value = pa.Series(np.nan, index=value.index, dtype=value.dtype)
                else:
                    self.current_value = np.ndarray(value.shape, dtype=value.dtype)
                    self.current_value.fill(np.nan)

        # update the current value
        if filter_node_value:
            self.send(value)

    def next(self):
        if self.is_float:
            return self.current_value_f
        return self.current_value.copy()

    def send(self, value):
        if self.is_float:
            # update the current value if value is not Nan
            value_f = cython.declare(cython.double, value)
            if value_f == value_f:
                self.current_value_f = value
            return self.current_value_f

        # update the current value with the non-nan values
        mask = ~np.isnan(value)
        self.current_value[mask] = value[mask]
        return self.current_value.copy()

# decorators don't work on cythoned types
ffillnode = nodetype(cls=MDFForwardFillNode, method="ffill")(_ffillnode)

class MDFReturnsNode(MDFCustomNode):
    pass

class _returnsnode(MDFIterator):
    """
    Decorator that creates an :py:class:`MDFNode` that returns
    the returns of a price series.

    NaN prices are filled forward.
    If there is a NaN price at the beginning of the series, we set
    the return to zero.
    The decorated function may return a float, pandas Series
    or numpy array.
    
    e.g.::
    
        @returnsnode
        def node():
            return some_price

    or using the nodetype method syntax (see :ref:`nodetype_method_syntax`)::

        @evalnode
        def some_price():
            return ...

        @evalnode
        def node():
            return some_price.returns()
            
    The value at any timestep is the return for that timestep, so the methods
    ideally would be called 'return', but that's a keyword and so returns is
    used.
    """
    _init_kwargs_ = ["filter_node_value"]

    def __init__(self, value, filter_node_value):
        self.is_float = False
        if isinstance(value, float):
            # floating point returns
            self.is_float = True
            self.prev_value_f = np.nan
            self.current_value_f = np.nan
            self.return_f = 0.0
        else:
            # Series or ndarray returns
            if not isinstance(value, (pa.Series, np.ndarray)):
                raise RuntimeError("returns node expects a float, pa.Series or ndarray") 
    
            if isinstance(value, pa.Series):
                self.prev_value = pa.Series(np.nan, index=value.index)
                self.current_value = pa.Series(np.nan, index=value.index)
            else:
                self.prev_value = np.ndarray(value.shape, dtype=value.dtype)
                self.current_value = np.ndarray(value.shape, dtype=value.dtype)
                self.returns = np.ndarray(value.shape, dtype=value.dtype)
                self.prev_value.fill(np.nan)
                self.current_value.fill(np.nan)
                self.returns.fill(0.0)

        # update the current value
        if filter_node_value:
            self.send(value)

    def next(self):
        if self.is_float:
            return self.return_f
        return self.returns

    def send(self, value):
        if self.is_float:
            value_f = cython.declare(cython.double, value)

            # advance previous to the current value and update current
            # value with the new value unless it's nan (in which case we
            # leave it as it is - ie fill forward).
            self.prev_value_f = self.current_value_f
            if value_f == value_f:
                self.current_value_f = value_f

            self.return_f = (self.current_value_f / self.prev_value_f) - 1.0
            if np.isnan(self.return_f):
                self.return_f = 0.0
            return self.return_f

        # advance prev_value and update current value with any new
        # non-nan values
        mask = ~np.isnan(value)
        self.prev_value = self.current_value.copy()
        self.current_value[mask] = value[mask]

        self.returns = (self.current_value / self.prev_value) - 1.0
        self.returns[np.isnan(self.returns)] = 0.0
        return self.returns

# decorators don't work on cythoned types
returnsnode = nodetype(cls=MDFReturnsNode, method="returns")(_returnsnode)

#
# datarownode is used to construct nodes from either DataFrames, WidePanels or
# TimeSeries.
#
class MDFRowIteratorNode(MDFCustomNode):
    # always pass index_node as the node rather than evaluate it
    nodetype_node_kwargs = ["index_node"]

class _rowiternode(MDFIterator):
    """
    Decorator that creates an :py:class:`MDFNode` that returns
    the current row of item of a pandas DataFrame, WidePanel
    or Series returned by the decoratored function.
    
    What row is considered current depends on the `index_node`
    parameter, which by default is `now`.
    
    `missing_value` may be specified as the value to use when
    the index_node isn't included in the data's index. The
    default is NaN. 
    
    `delay` can be a number of timesteps to delay the index_node
    by, effectively shifting the data.
    
    `ffill` causes the value to get forward filled if True, default is False.
    
    e.g.::
    
        @rowiternode
        def datarow_node():
            # construct a dataframe indexed by date
            return a_dataframe

        @evalnode
        def another_node():
            # the rowiternode returns the row from the dataframe
            # for the current date 'now'
            current_row = datarow_node()

    or using the nodetype method syntax (see :ref:`nodetype_method_syntax`)::

        @evalnode
        def dataframe_node():
            # construct a dataframe indexed by date
            return a_dataframe

        @evalnode
        def another_node():
            # get the row from dataframe_node for the current_date 'now'
            current_row = dataframe_node.rowiter()
    """
    _init_kwargs_ = ["owner_node", "index_node", "missing_value", "delay", "ffill"]

    def __init__(self, data, owner_node, index_node=now, missing_value=np.nan, delay=0, ffill=False):
        """data should be a dataframe, widepanel or timeseries"""
        self._current_index = None
        self._current_value = None
        self._prev_value = None
        self._missing_value_orig = missing_value
        self._index_to_date = False
        self._ffill = ffill

        # call the index node to make sure this node depends on it and remember the type
        index_value = index_node()
        self._index_node_type = type(index_value)

        # store the node and delay it if necessary
        self._index_node = index_node
        if delay > 0:
            self._index_node = self._index_node.delaynode(periods=delay,
                                                          filter=owner_node.get_filter())

        self._set_data(data)

    def _set_data(self, data):
        self._data = data

        self._is_dataframe = False
        self._is_widepanel = False
        self._is_series = False

        # this may get updated (e.g. to be a series corresponding to the columns
        # of a dataframe) so restore it to the original value.
        self._missing_value = self._missing_value_orig

        try:
            if isinstance(data, pa.DataFrame):
                self._is_dataframe = True

                # convert missing value to a row with the same columns as the dataframe
                if not isinstance(self._missing_value, pa.Series):
                    dtype = object
                    if data.index.size > 0:
                        dtype = data.xs(data.index[0]).dtype
                    self._missing_value = pa.Series(self._missing_value,
                                                    index=data.columns,
                                                    dtype=dtype)

                # set up the iterator
                self._iter = iter(data.index)
                self._current_index = next(self._iter)
                self._current_value = self._data.xs(self._current_index)

            elif isinstance(data, pa.WidePanel):
                self._is_widepanel = True

                # convert missing value to a dataframe with the same dimensions as the panel
                if not isinstance(self._missing_value, pa.DataFrame):
                    if not isinstance(self._missing_value, dict):
                        self._missing_value = dict([(c, self._missing_value) for c in data.items])
                    self._missing_value = pa.DataFrame(self._missing_value,
                                                        columns=data.items,
                                                        index=data.minor_axis,
                                                        dtype=data.dtype)

                # set up ther iterator
                self._iter = iter(data.major_axis)
                self._current_index = next(self._iter)
                self._current_value = self._data.major_xs(self._current_index)

            elif isinstance(data, pa.Series):
                self._is_series = True
                self._iter = _dict_iteritems(data)
                self._current_index, self._current_value = next(self._iter)

            else:
                clsname = type(data)
                if hasattr(data, "__class__"):
                    clsname = data.__class__.__name__
                raise AssertionError("datanode expects a DataFrame, WidePanel or Series; "
                                     "got '%s'" % clsname)

        except StopIteration:
            self._current_index = None
            self._current_value = self._missing_value

        # reset _prev_value to the missing value, it will get set as the iterator is advanced.
        self._prev_value = self._missing_value

        # does the index need to be converted from datetime to date?
        # (use the stored index_node_type as the current value may be delayed
        # and therefore be None instead of it usual type)
        self._index_to_date = type(self._current_index) is datetime.date \
                                and self._index_node_type is datetime.datetime

    def send(self, data):
        if data is not self._data:
            self._set_data(data)
        return self.next()

    def next(self):
        # switching this way cythons better than having a python
        # function object and calling that, because it doesn't have
        # the overhead of doing a python object call.
        if self._is_dataframe:
            return self._next_dataframe()
        if self._is_widepanel:
            return self._next_widepanel()
        if self._is_series:
            return self._next_series()
        return self._missing_value

    def _next_dataframe(self):
        i = self._index_node()
        if self._current_index is None \
        or i is None:
            return self._missing_value

        if self._index_to_date:
            i = i.date()

        while i > self._current_index:
            # advance to the next item in the series
            try:
                # TODO: once we upgrade pandas use the iterrows method
                self._prev_value = self._current_value
                self._current_index = next(self._iter)
                self._current_value = self._data.xs(self._current_index)
            except StopIteration:
                if self._ffill:
                    return self._current_value
                return self._missing_value

        if self._current_index == i:
            return self._current_value

        if self._ffill and self._current_index > i:
            return self._prev_value

        return self._missing_value

    def _next_widepanel(self):
        i = self._index_node()
        if self._current_index is None \
        or i is None:
            return self._missing_value

        if self._index_to_date:
            i = i.date()

        while i > self._current_index:
            # advance to the next item in the series
            try:
                # TODO: once we upgrade pandas use the iterrows method
                self._prev_value = self._current_value
                self._current_index = next(self._iter)
                self._current_value = self._data.major_xs(self._current_index)
            except StopIteration:
                if self._ffill:
                    return self._prev_value
                return self._missing_value

        if self._current_index == i:
            return self._current_value

        if self._ffill and self._current_index > i:
            return self._prev_value

        return self._missing_value

    def _next_series(self):
        i = self._index_node()
        if self._current_index is None \
        or i is None:
            return self._missing_value

        if self._index_to_date:
            i = i.date()

        while i > self._current_index:
            # advance to the next item in the series
            try:
                self._prev_value = self._current_value
                self._current_index, self._current_value = next(self._iter)
            except StopIteration:
                if self._ffill:
                    return self._prev_value
                return self._missing_value

        if self._current_index == i:
            return self._current_value

        if self._ffill and self._current_index > i:
            return self._prev_value

        return self._missing_value

# decorators don't work on cythoned types
rowiternode = nodetype(cls=MDFRowIteratorNode, method="rowiter")(_rowiternode)

#
# helper function for creating a row iterator node, but without having
# to write the function just to return a dataframe/series etc...
#
def datanode(name=None,
             data=None,
             index_node=now,
             missing_value=np.nan,
             delay=0,
             ffill=False,
             filter=None,
             category=None):
    """
    Return a new mdf node for iterating over a dataframe, panel or series.
    
    `data` is indexed by another node `index_node`, (default is :py:func:`now`),
    which can be any node that evaluates to a value that can be used to index
    into `data`.
    
    If the `index_node` evaluates to a value that is not present in
    the index of the `data` then `missing_value` is returned.

    `missing_value` can be a scalar, in which case it will be converted
    to the same row format used by the data object with the same value
    for all items.
    
    `delay` can be a number of timesteps to delay the index_node
    by, effectively shifting the data.
    
    `ffill` causes the value to get forward filled if True, default is False.
    
    `data` may either be a data object itself (DataFrame, WidePanel or
    Series) or a node that evaluates to one of those types.
    
    e.g.::
 
        df = pa.DataFrame({"A" : range(100)}, index=date_range)
        df_node = datanode(data=df)

        ctx[df_node] # returns the row from df where df == ctx[now]

    A datanode may be explicitly named using the name argument, or
    if left as None the variable name the node is being assigned to
    will be used.
    """
    assert data is not None, "Must specify data as a DataFrame, Series or node"

    if name is None:
        name = get_assigned_node_name("datanode", 0 if cython.compiled else 1)

    if isinstance(data, MDFNode):
        func = MDFCallable(name, data)
    else:
        func = MDFCallable(name, lambda: data)

    node = MDFRowIteratorNode(name=name,
                              func=func,
                              node_type_func=_rowiternode,
                              category=category,
                              filter=filter,
                              nodetype_func_kwargs={
                                "index_node" : index_node,
                                "delay" : delay,
                                "missing_value" : missing_value,
                                "ffill" : ffill,
                              })
    return node

def filternode(name=None,
               data=None,
               index_node=now,
               delay=0,
               filter=None,
               category=None):
    """
    Return a new mdf node for using as a filter for other nodes
    based on the index of the data object passed in (DataFrame,
    Series or WidePanel).
    
    The node value is True when the index_node (default=now)
    is in the index of the data, and False otherwise.
    
    This can be used to easily filter other nodes so that
    they operate at the same frequency of the underlying data.
    
    `delay` can be a number of timesteps to delay the index_node
    by, effectively shifting the data.

    A filternode may be explicitly named using the name argument, or
    if left as None the variable name the node is being assigned to
    will be used.
    """
    assert data is not None, "Must specify data as a DataFrame, Series or node"

    # the filter is always True for points on the data's index,
    # and False otherwise.
    func = lambda: pa.Series(True, index=data.index)
    if isinstance(data, MDFNode):
        func = lambda: pa.Series(True, index=data().index)

    if name is None:
        name = get_assigned_node_name("filternode", 0 if cython.compiled else 1)

    if isinstance(data, MDFNode):
        func = MDFCallable(name, data, lambda x: pa.Series(True, index=x.index))
    else:
        func = MDFCallable(name, lambda: pa.Series(True, index=data.index))

    node = MDFRowIteratorNode(name=name,
                              func=MDFCallable(name, func),
                              node_type_func=_rowiternode,
                              category=category,
                              filter=filter,
                              nodetype_func_kwargs={
                                "index_node" : index_node,
                                "missing_value" : False,
                                "delay" : delay,
                              })
    return node

#
# applynode is a way of transforming a plain function into an mdf
# node by binding other nodes to its parameters.
# This is useful for quick interactive work more than for applications
# written using mdf. 
#
class MDFApplyNode(MDFCustomNode):
    nodetype_kwargs = ["func", "args", "kwargs"]

def _applynode(value, func, args=(), kwargs={}):
    """
    Return a new mdf node that applies `func` to the value of the node
    that is passed in. Extra `args` and `kwargs` can be passed in as
    values or nodes.
    
    Unlike most other node types this shouldn't be used as a decorator, but instead
    should only be used via the method syntax for node types, (see :ref:`nodetype_method_syntax`)
    e.g.::
    
        A_plus_B_node = A.applynode(operator.add, args=(B,))
    
    """
    new_args = []
    for arg in args:
        if isinstance(arg, MDFNode):
            arg = arg()
        new_args.append(arg)

    new_kwargs = {}
    for key, value in _dict_iteritems(kwargs):
        if isinstance(value, MDFNode):
            value = value()
        new_kwargs[key] = value

    return func(value, *new_args, **new_kwargs)

# decorators don't work on cythoned types
applynode = nodetype(cls=MDFApplyNode, method="apply")(_applynode)

#
# lookaheadnode evaluates a node over a date range or for a number
# of periods in the future and returns a pandas series of values.
# When looking for a number of periods in the future it does that
# for the first timestep only and is constant thereafter.
# It's intended use is for small look aheads for seeding moving
# average type calculations.
#
class MDFLookAheadNode(MDFCustomNode):
    nodetype_kwargs = ["value", "owner_node", "periods", "filter_node", "offset"]
    
    # don't mark this node as dirty when dependent nodes are dirtied
    # because of changes to the current date.
    dirty_flags_propagate_mask = ~DIRTY_FLAGS.TIME

    def on_set_date(self, ctx, date):
        """called just before 'now' is changed"""
        # return True if date is going backwards to indicate we should be marked as dirty
        return ctx.get_date() > date

def _lookaheadnode(value_unused, owner_node, periods, filter_node=None, offset=pa.datetools.BDay()):
    """
    Node type that creates an :py:class:`MDFNode` that returns
    a pandas Series of values of the underlying node for a sequence
    of dates in the future.
    
    Unlike most other node types this shouldn't be used as a decorator, but instead
    should only be used via the method syntax for node types, (see :ref:`nodetype_method_syntax`)
    e.g.::
    
        future_values = some_node.lookahead(periods=10)

    This would get the next 10 values of ``some_node`` after the current date. Once
    evaluated it won't be re-evaluated as time moves forwards; it's always the first
    set of future observations. It is intended to be used sparingly for seeding
    moving average calculations or other calculations that need some initial value
    based on the first few samples of another node.
    
    The dates start with the current context date (i.e. :py:func:`now`) and is
    incremented by the optional argument `offset` which defaults to weekdays
    (see :py:class:`pandas.datetools.BDay`).

    :param int periods: the total number of observations to collect, excluding any that are ignored due
                        to any filter being used.

    :param offset: date offset object (e.g. datetime timedelta or pandas date offset) to use to
                   increment the date for each sample point.
    
    :param filter: optional node that if specified should evaluate to True if an observation is to
                   be included, or False otherwise.

    """
    assert owner_node.base_node is not None, \
        "lookahead nodes must be called via the lookahead or lookaheadnode methods on another node"

    ctx = cython.declare(MDFContext)
    shifted_ctx = cython.declare(MDFContext)

    # create a shifted context from the current context shifted by date
    ctx = _get_current_context()
    date = ctx.get_date()
    shifted_ctx = ctx.shift({now : date})

    # collect results from the shifted context
    count = cython.declare(int, 0)
    values = cython.declare(list, [])
    dates = cython.declare(list, [])

    try:
        while count < periods:
            shifted_ctx.set_date(date)
            date += offset
    
            if filter_node is not None:
                if not shifted_ctx.get_value(filter_node):
                    continue
    
            value = shifted_ctx.get_value(owner_node.base_node)
            values.append(value)
            dates.append(shifted_ctx.get_date())
            count += 1
    finally:
        # removed any cached values from the context since they won't be needed again
        # and would otherwise just be taking up memory.
        shifted_ctx.clear()

    if count > 0 and isinstance(values[0], pa.Series):
        return pa.DataFrame(values, index=dates)

    return pa.Series(values, index=dates)
     

# decorators don't work on cythoned classes
lookaheadnode = nodetype(_lookaheadnode, cls=MDFLookAheadNode, method="lookahead")

class Op(object):
    op = cython.declare(object)
    lhs = cython.declare(object)    

    def __init__(self, op, lhs=None):
        self.op = op
        self.lhs = lhs

    def __get__(self, instance, owner=None):
        if instance is not None:
            return self.__class__(self.op, instance)
        return self.__class__(self.op, owner)

    def __call__(self, rhs=None):
        args = ()
        if rhs is not None:
            args = (rhs,)
        return self.lhs.applynode(func=self.op, args=args)

if sys.version_info[0] <= 2:
    for op in ("__add__", "__sub__", "__mul__", "__div__", "__neg__"):
        MDFNode._additional_attrs_[op] = Op(getattr(operator, op)) 
else:
    for op in ("__add__", "__sub__", "__mul__", "__truediv__", "__neg__"):
        MDFNode._additional_attrs_[op] = Op(getattr(operator, op))
