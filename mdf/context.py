import time
import itertools
import os
from datetime import datetime
import cython
import warnings
import sys
from .common import DIRTY_FLAGS
from . import io

# this is usually cimported in context.pxd
# uncomment if not compiling with Cython
#from cqueue import *
#import thread
#PyThread_get_thread_ident = thread.get_ident

DIRTY_FLAGS_NONE = cython.declare(int, DIRTY_FLAGS.NONE)
DIRTY_FLAGS_ALL  = cython.declare(int, DIRTY_FLAGS.ALL)
DIRTY_FLAGS_TIME = cython.declare(int, DIRTY_FLAGS.TIME)

_python_version = cython.declare(int, sys.version_info[0])

# imported when MDFContext is constructed
MDFNode = None
_now_node = None
_pickle_context = None
_unpickle_context = None
_pickle_shift_set = None
_unpickle_shift_set = None

_profiling_enabled = cython.declare(int, False)
def enable_profiling(enable=True):
    global _profiling_enabled
    _profiling_enabled = enable

def _profiling_is_enabled():
    return _profiling_enabled

_allow_duplicate_nodes = cython.declare(int, False)
def allow_duplicate_nodes(enable=True):
    """
    Nodes with the same full name (including module and class names)
    are not allowed because it means they can't be differentiated
    correctly when unpickling.
    
    However when reloading modules duplicate nodes occur because
    existing nodes are redeclared. This is something that only
    happens during development, and so this function is here to
    allow that, but the default is not to allow it.
    """
    global _allow_duplicate_nodes
    _allow_duplicate_nodes = enable

class DuplicateNodeError(Exception):
    def __init__(self, node):
        Exception.__init__(self,
                            ("A node named '%s' already exists\n" % node.name) +
                                "If you are reloading a module and want "
                                "duplicate nodes to be allowed call "
                                "mdf.allow_duplicate_nodes().")

class NoCurrentContextError(RuntimeError):
    def __init__(self):
        RuntimeError.__init__(self, "No current context")

class Cookie(object):
    def __init__(self, thread_id, prev_context):
        self.thread_id = thread_id
        self.prev_context = prev_context

class MDFNodeBase(object):
    """
    Trivial class that MDFNode implements.
    This is purely so cython can optimize some access
    of node properties and methods from this file without introducing
    a circular module dependency.
    """
    # cdef bint _has_set_date_callback
    # cdef bint _has_timestep_update

    def _add_dependency(self, ctx, called_node, called_ctx):
        raise NotImplementedError()

    def get_dependencies(self, ctx):
        raise NotImplementedError()

    def get_alt_context(self, ctx):
        raise NotImplementedError()

    def get_value(self, ctx, thread_id=None):
        raise NotImplementedError()
        
    def has_value(self, ctx):
        raise NotImplementedError()
        
    def set_dirty(self, ctx, flags=DIRTY_FLAGS.ALL):
        raise NotImplementedError()

    def clear(self, ctx):
        raise NotImplementedError()

    def clear_value(self, ctx):
        raise NotImplementedError()

    def set_value(self, ctx, value):
        raise NotImplementedError()

    def set_override(self, ctx, override_node):
        raise NotImplementedError()

def _lazy_imports():
    # import MDFNode after this module has been imported
    # to avoid circular import dependencies
    global MDFNode, _now_node
    import nodes
    MDFNode = nodes.MDFNode
    _now_node = nodes._now_node

    global _pickle_context, _unpickle_context
    import ctx_pickle
    _pickle_context = ctx_pickle._pickle_context
    _unpickle_context = ctx_pickle._unpickle_context

    global _pickle_shift_set, _unpickle_shift_set
    _pickle_shift_set = ctx_pickle._pickle_shift_set
    _unpickle_shift_set = ctx_pickle._unpickle_shift_set

class Timer(object):
    """object for collecting metrics about nodes and builders"""
    def __init__(self, node_or_builder):
        self.node_or_builder = node_or_builder
        self.num_calls = 0
        self.total_time = 0.0
        self.is_running = False

    def start(self):
        assert not self.is_running
        self.num_calls += 1
        self.is_running = True
        self.started_time = time.clock()

    def resume(self):
        if not self.is_running:
            self.started_time = time.clock()
            self.is_running = True

    def stop(self, stop_time):
        assert self.is_running
        if stop_time < 0.0:
            stop_time = time.clock()
        self.total_time += stop_time - self.started_time
        self.is_running = False

class NodeOrBuilderTimer(object):
    """object with with semantics for timing a node or builder"""
    def __init__(self, ctx, node_or_builder):
        self.ctx = ctx
        self.node_or_builder = node_or_builder

    def __enter__(self):
        return self.ctx._start_timer(self.node_or_builder)

    def __exit__(self, exc_type, exc_value, traceback):
        self.ctx._stop_timer()

class NullTimer:
    """used in place of NodeOrBuilderTimer when profiling isn't enabled"""
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

# use a single instance of the null timer to avoid cost of construction
_null_timer = NullTimer()

# this dict is the set of default nodes common to all
# contexts. It gets added to as MDFNode instances are
# constructed via register_node.
_all_nodes = cython.declare(dict, {})
_current_contexts = cython.declare(dict, {})
_ctx_id_counter = itertools.count()

class NowNodeValue:
    """
    the now node is special as it's the only node that may be shifted
    on that remains mutable after the shift.
    Because of that, each context shifted on now needs a unique object
    wrapping the now value - even for shifts that initially have the
    same date.
    """
    
    def __init__(self, value):
        self.value = value

    def __reduce__(self):
        """support for pickling"""
        return (
            _nownodevalue_unpickle,
            (self.value,),
            None,
            None,
            None,
        )

    def __str__(self):
        return self.value.strftime("%Y-%m-%d")

def _nownodevalue_unpickle(value):
    return NowNodeValue(value)

def _make_shift_key(shift_set):
    key = [(id(node), value) for (node, value) in shift_set.iteritems()]
    key.sort()
    return tuple(key)

class ShiftSet(dict):
    """
    dict subclass that encapsulates the net shift set for a given
    shift set and the context it's being applied to.
    
    This is used as an optimization when shifting contexts as part of
    an mdf graph to avoid having to recompute the shift key on each
    iteration. 

    See also :py:func:`make_shift_key` and :py:func:`shift`.
    """

    def __init__(self, shift_set):
        dict.__init__(self, shift_set)

        # if being shifted by now then wrap the value as it needs
        # to be a unique per shift set because now is mutable even in
        # shifted contexts
        if _now_node in self:
            now_value = self[_now_node]
            if not isinstance(now_value, NowNodeValue):
                self[_now_node] = NowNodeValue(now_value)

        # cache for _get_shift_key
        self._shift_keys = {}

    def __reduce__(self):
        """support for pickling"""
        return (
            _unpickle_shift_set,
            _pickle_shift_set(self),
            None,
            None,
            None,
        )

    def _get_shift_key(self, ctx):
        try:
            return self._shift_keys[ctx._id_obj]
        except KeyError:
            pass

        # create the net shift of self and the context's shift set (applying
        # self to the ctx's shift set so any re-shifted nodes take the values
        # from the shift set).
        net_shift_set = cython.declare(dict)
        net_shift_set = dict(ctx._shift_set)
        net_shift_set.update(self)

        # get the shift key and add it to the cache
        shift_key = _make_shift_key(net_shift_set)
        self._shift_keys[ctx._id_obj] = shift_key
        return shift_key

class MDFContext(object):
    """
    Nodes on their own don't have values, they are just the things
    that can calculate values.

    Nodes only have values *in a context*.

    Contexts can be thought of as containers for values of nodes.
    """

    _dot_colors = {
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
        "module1"   :   "grey72",
    }

    def __init__(self, now=None,
                _shift_parent=None,
                _shift_set={},
                _cache_shifted=True):
        """
        Initializes a new context with :py:func:`now` set to now (datetime).
        """
        if now is None:
            now = datetime.now()
        self._init(now, _shift_parent, _shift_set, _cache_shifted)

    def __reduce__(self):
        """support for pickling"""
        return (
            _unpickle_context,
            _pickle_context(self),
            None,
            None,
            None,
        )

    def _init(self, now,
              _shift_parent=None,
              _shift_set={},
              _cache_shifted=True):
        """
        _shift_parent: context this context is a shift of (shifting)
        _shift_target: node to be shifted node when _shift_parent is set
        _shift_value: value of shifted node when _shift_parent is set
        """
        self._finalized = False
        self._id = self._id_obj = next(_ctx_id_counter)
        self._now = now
        self._incrementally_updated_nodes = {}
        self._has_incrementally_updated_nodes = False
        self._nodes_requiring_set_date_callback = {}
        self._has_nodes_requiring_set_date_callback = False
        self._node_eval_stack = cqueue()
        self._timers = {}
        self._timer_stack = []
        self._parent = None

        node = cython.declare(MDFNodeBase)

        # shifted contexts are only one level deep, so get the parent from
        # the shift parent to get this context's shift parent
        parent = cython.declare(MDFContext)
        parent = _shift_parent
        if parent is not None:
            while parent._parent is not None:
                parent = parent._parent
            self._parent = parent

        # a context can be 'shifted' which means that it's a shallow copy
        # of an existing context but with one or more values changed.
        self._shifted_cache = {}
        self._shifted_contexts = {}
        self._all_child_contexts = {}
        self._shift_set = {}
        self._is_shift_of_cache = {}
        
        # a hashable key (tuple) is stored to identify the shift set for this context,
        # and also a set of that tuple to be used by the 'is_shift_of' method to avoid
        # constructing a set each time it's called which is costly when there are large
        # numbers of shifted contexts.
        self._shift_key = tuple()
        self._shift_key_set = set()

        # add self to parents's dict of all contexts
        if self._parent is not None:
            self._parent._all_child_contexts[self._id_obj] = self

        if _shift_parent and _shift_set:
            assert _shift_parent._now == now, "Can't shift from a context with a different date"

            # all the shifts from the parent apply to this context too as
            # well as the newly shifted node
            self._shift_set.update(_shift_parent._shift_set)
            self._shift_set.update(_shift_set)

            self._shift_key = _make_shift_key(self._shift_set)
            self._shift_key_set = set(self._shift_key)

            if _cache_shifted:
                # cache self on the parent in case the same shift is applied again
                # shifting is commutative so the order the shifts are applied in doesn't matter 
                parent._shifted_cache[self._shift_key] = self

            # add a ref of self to the parent so changed applied to the parent are propagated to self
            # this is a non-weak reference; shifted contexts are always referenced until the parent
            # context is destroyed.
            parent._shifted_contexts[self] = None

            # set the shifted values in the context
            # - unless it's the now node, as it gets set below only for the root contexts
            # - or unless the shifted node already has an alt context other than this one,
            # as it must already be set in that context and so there's no need to set
            # it in this one.
            for node, value in self._shift_set.iteritems():
                if node is not _now_node:
                    if isinstance(value, MDFNode) \
                    or node.get_alt_context(self) is self:
                        self[node] = value

        # if this context is a root context set the now node's value
        if _shift_parent is None \
        or (len(self._shift_set) == 1 and _now_node in self._shift_set):
            self[_now_node] = self._shift_set.get(_now_node, self._now)

        # stop further modifications to the context, if it's shifted
        self._finalized = True

    #
    # methods for loading and saving contexts (see io sub-package)
    #
    save = io.save_context
    load = staticmethod(io.load_context)

    def get_id(self):
        """returns a unique id for this context"""
        return self._id

    def clear(self):
        """
        clears all cached data for this context
        """
        for node in _all_nodes.itervalues():
            node.clear(self)

        self._incrementally_updated_nodes.clear()
        self._has_incrementally_updated_nodes = False
        
        self._nodes_requiring_set_date_callback.clear()
        self._has_nodes_requiring_set_date_callback = False

        # clear the shifted contexts
        for shifted_ctx in self._shifted_cache.itervalues():
            shifted_ctx.clear()

        # remove any references to shifted contexts
        self._shifted_cache.clear()
        self._shifted_contexts.clear()
        self._all_child_contexts.clear()
        self._is_shift_of_cache.clear()

        self._node_eval_stack = cqueue()

    def __del__(self):
        # in some cases _all_nodes has been deleted by the time the context is
        # recycled (e.g. in IPython)
        if not _all_nodes:
            return
        # clear any nodes that have cached state for this context
        for node in _all_nodes.itervalues():
            node.clear(self)

    def __str__(self):
        if self._shift_set:
            # don't use ctx.get_value here as it adds dependencies
            shifts = sorted([(node.name, node, shift) for (node, shift) in self._shift_set.items()])
            shifts = [(node, shift) for (name, node, shift) in shifts]
            shifts_strs = []

            for node, value in shifts:
                if isinstance(value, MDFNode):
                    value = value.name.rsplit(".")[-1]

                # string format it sensibly
                if isinstance(value, datetime):
                    value = value.strftime("%Y-%m-%d")
                value = str(value)
                if len(value) > 10:
                    value = value[:10] + "..."

                # add the k=v string to be printed later
                shifted_name = node.name.rsplit(".")[-1]
                shifts_strs.append("%s=%s" % (shifted_name, value))

            return "<ctx %d: %s [%s]>" % (self._id,
                                          self._now.strftime("%Y-%m-%d"),
                                          ", ".join(shifts_strs))
        return "<ctx %d: %s>" % (self._id, self._now.strftime("%Y-%m-%d"))

    def __repr__(self):
        return "<%s at %d>" % (str(self).strip("<>"), id(self))

    def _shift(self, shift_set_, cache_context=True):
        """
        see shift - this is the implementation called from C, split
        out so other C functions can call this directly
        """
        # if there's nothing to shift return this context
        if not shift_set_:
            return self

        # check the type of shift_set_ and construct a new ShiftSet if required
        shift_set = cython.declare(ShiftSet)
        if not isinstance(shift_set_, ShiftSet):
            shift_set = ShiftSet(shift_set_)
        else:
            shift_set = shift_set_

        # if a context already exists for this shift set then return it.
        parent = cython.declare(MDFContext)
        parent = self._parent or self
        try:
            shift_key = shift_set._get_shift_key(self)
            return parent._shifted_cache[shift_key]
        except KeyError:
            pass

        # return a new shifted context
        return MDFContext(self._now,
                          _shift_parent=self,
                          _shift_set=shift_set,
                          _cache_shifted=cache_context)

    def shift(self, shift_set, cache_context=True):
        """
        create a new context linked to this context, but
        with nodes set to specific values.
        
        ``shift_set`` is a dictionary of nodes to values.
        The returned shifted context will have each node
        in the dictionary set to its corresponding value.

        If the same shift set is used several times a `ShiftSet`
        object may be used instead of a dictionary which will
        be slightly faster. See :py:func:`make_shift_set`.

        If a value is an MDFNode then it will be applied
        as an override to the target node.

        If a context has already been created with this
        shift that existing context is returned instead.

        Shifted contexts are read-only.

        If cache_context is True the shifted context will be
        cached and if :py:meth:`shift` is called again with the same
        target and value the cached context will be returned.
        The exception to this is if ``target`` is :py:func:`now`, in 
        which case a new shifted context is returned each time.
        """
        return self._shift(shift_set, cache_context)

    def get_parent(self):
        return self._parent

    def get_shift_set(self):
        return self._shift_set

    def get_shifted_contexts(self):
        if _python_version > 2:
            return list(self._shifted_contexts.keys())
        return self._shifted_contexts.keys()

    def iter_shifted_contexts(self):
        if _python_version > 2:
            return self._shifted_contexts.keys()
        return self._shifted_contexts.iterkeys()

    @classmethod
    def register_node(cls, node):
        # Check the node hasn't already been registered.
        # If it has it's possibly a node that's been named badly and
        # will cause problems when pickling/unpickling if not renamed
        # so it's unique.
        if not _allow_duplicate_nodes: 
            if (node.name, node.is_bound) in _all_nodes:
                raise DuplicateNodeError(node)

        # add the new node to the dict
        _all_nodes[(node.name, node.is_bound)] = node

    @classmethod
    def unregister_node(cls, node):
        # remove a node from that global list of known nodes.
        # this should be incredibly rarely used and was only
        # added to test cases where nodes don't exist when
        # un-pickling.
        _all_nodes.pop((node.name, node.is_bound), None)

    @property
    def now(self):
        """see get_date"""
        return self.get_date()

    @now.setter
    def now(self, value):
        """see set_date"""
        self.set_date(value)

    def get_date(self):
        """
        returns the current date set on this context.

        This is equivalent to getting the value of the
        :py:func:`now` node in this context.
        """
        return self._now

    def is_shift_of(self, other):
        """
        returns True if this context's shift set is a super-set of
        the other context's shift set
        """
        try:
            return self._is_shift_of_cache[other._id_obj]
        except KeyError:
            pass

        if self._parent is not (other._parent or other) \
        and self is not other:
            return False

        self_key = self._shift_key_set
        other_key = other._shift_key_set

        # if the keys are the same both contexts are shifts of each other        
        if self_key == other_key:
            self._is_shift_of_cache[other._id_obj] = True
            other._is_shift_of_cache[self._id_obj] = True
            return True

        # if self is a shift of other then other may not be a shift of self
        if self_key.issuperset(other_key):
            self._is_shift_of_cache[other._id_obj] = True 
            other._is_shift_of_cache[self._id_obj] = False
            return True

        # self is not a shift of other, but other may be a shift of self
        self._is_shift_of_cache[other._id_obj] = False
        return False

    def _set_date(self, date):
        """
        implementation for set_date
        """
        # unwrap if necessary
        if isinstance(date, NowNodeValue):
            tmp = cython.declare(NowNodeValue)
            tmp = date
            date = tmp.value

        if date == self._now:
            return

        # remember the date before it's changed
        prev_date = self._now

        # don't allow the date to be changed on a shifted context as it will
        # potentially update values in the context below
        if self._shift_set and _now_node not in self._shift_set:
            raise Exception("Can't change the date on a shifted context")

        ctx = cython.declare(MDFContext)
        parent = cython.declare(MDFContext)
        prev_ctx = cython.declare(MDFContext)
        shifted_ctx = cython.declare(MDFContext)
        cookie = cython.declare(Cookie)
        node = cython.declare(MDFNodeBase)

        # get the prev_ctx and thread_id by activating the current context
        cookie = self._activate(None, None)
        thread_id = cookie.thread_id
        prev_ctx = cookie.prev_context
        self._deactivate(cookie)

        # get a list of all the contexts that are shifted by the same now
        # node as this context
        parent = self._parent if self._parent is not None else self
        all_shifted_contexts = parent.get_shifted_contexts()

        # create all_contexts as a list with the max number of elements potentially required
        all_contexts = cython.declare(list)
        all_contexts = [None] * (len(all_shifted_contexts) + 1)
        all_contexts[0] = self

        num_contexts = cython.declare(int)
        num_contexts = 1

        contexts_with_set_date_callbacks = cython.declare(list, [])
        have_on_set_date_callbacks = cython.declare(int, False)
        if self._has_nodes_requiring_set_date_callback:
            contexts_with_set_date_callbacks.append(self)
            have_on_set_date_callbacks = True

        if _now_node in self._shift_set:
            shifted_now = self._shift_set[_now_node]
            for shifted_ctx in all_shifted_contexts:
                shifted_shift_set = shifted_ctx.get_shift_set()
                try:
                    if shifted_shift_set[_now_node] is shifted_now:
                        if shifted_ctx is not self:
                            all_contexts[num_contexts] = shifted_ctx
                            num_contexts += 1
                            if shifted_ctx._has_nodes_requiring_set_date_callback:
                                contexts_with_set_date_callbacks.append(shifted_ctx)
                                have_on_set_date_callbacks = True
                except KeyError:
                    pass
        else:
            # now hasn't been shifted in this context so include all
            # other contexts also not shifted by now
            for shifted_ctx in all_shifted_contexts:
                if _now_node not in shifted_ctx.get_shift_set():
                    if shifted_ctx is not self:
                        all_contexts[num_contexts] = shifted_ctx
                        num_contexts += 1
                        if shifted_ctx._has_nodes_requiring_set_date_callback:
                            contexts_with_set_date_callbacks.append(shifted_ctx)
                            have_on_set_date_callbacks = True

        # trim any unused slots
        all_contexts = all_contexts[:num_contexts]

        # call the 'on_set_date' callback on any nodes needing it before
        # actually setting the date on the context.
        # If on_set_date returns True that indicates the node will become dirty
        # once the date has been changed.
        on_set_date_dirty = cython.declare(list)
        on_set_date_dirty_count = cython.declare(int, 0)
        if have_on_set_date_callbacks:
            on_set_date_dirty = []

            for ctx in contexts_with_set_date_callbacks:
                # get the calling node and activate the context once and for all nodes
                calling_node = ctx._get_calling_node(prev_ctx)
                cookie = ctx._activate(prev_ctx, thread_id)
                try:
                    # call the callbacks (this may call other nodes and so might
                    # modify the set of nodes with callbacks)
                    for node in ctx._nodes_requiring_set_date_callback.keys():
                        cqueue_push(ctx._node_eval_stack, node)
                        try:
                            with ctx._profile(node) as timer:
                                dirty = node.on_set_date(ctx, date)
                            if dirty:
                                on_set_date_dirty.append((node, ctx))
                                on_set_date_dirty_count += 1
                        finally:
                            cqueue_pop(ctx._node_eval_stack)
                finally:
                    ctx._deactivate(cookie)

        # now all the on_set_date callbacks have been called update the date
        # for each context and mark any incrementally updated nodes as dirty.
        for ctx in all_contexts:
            # set now on the context
            ctx._now = date

            # mark any incrementally updated nodes as dirty
            if ctx._has_incrementally_updated_nodes:
                for node in ctx._incrementally_updated_nodes.iterkeys():
                    node.set_dirty(ctx, DIRTY_FLAGS_TIME)

        # mark any nodes that indicated they would become dirty after calling 'on_set_date'
        if on_set_date_dirty_count > 0:
            for node, ctx in on_set_date_dirty:
                node.set_dirty(ctx, DIRTY_FLAGS_TIME)

        # set the now node value in the least shifted context
        # (anything dependent on now will be dependent on
        # it in this context so no need to touch it in the shifted contexts)
        alt_ctx = _now_node.get_alt_context(self)
        alt_ctx.set_value(_now_node, date)

        if date < prev_date:
            # if setting the date to a date in the past clear any incrementally
            # updated nodes so they'll start from their initial values again
            for ctx in all_contexts:
                # clear any cached values for the incrementally updated nodes
                for node in ctx._incrementally_updated_nodes.iterkeys():
                    node.clear_value(ctx)

                # these will be re-established as the date is incremented
                ctx._incrementally_updated_nodes.clear()
                ctx._has_incrementally_updated_nodes = False

                ctx._nodes_requiring_set_date_callback.clear()
                ctx._has_nodes_requiring_set_date_callback = False

            return

        # Evaluate any nodes that have to be updated incrementally each timestep.
        # They're marked as dirty first as they should be called every timestep
        # regardless of whether anything underneath has changed.
        #
        # this is done in two phases, setting flags then updating so that
        # if one node depends on another and causes it to get evaluated
        # it doesn't get called twice.
        # (the flags are already set in the loop previous to this one)
        for ctx in all_contexts:
            if not ctx._has_incrementally_updated_nodes:
                continue

            # get the calling node and activate the context once and for all nodes
            calling_node = ctx._get_calling_node(prev_ctx)
            cookie = ctx._activate(prev_ctx, thread_id)

            try:
                # get the value to trigger the update
                for node in ctx._incrementally_updated_nodes.keys():
                    ctx._get_node_value(node, calling_node, ctx, thread_id)
            finally:
                ctx._deactivate(cookie)

    def set_date(self, date):
        """
        sets the current date set on this context.

        This updates the value of the :py:func:`now` node in this
        context and also calls the update functions for any previously
        evaluated time-dependent nodes in this context.
        """
        cookie = self._activate()
        try:
            self._set_date(date)
        finally:
            self._deactivate(cookie)

    def _activate_ctx(self, prev_ctx=None, thread_id=None):
        return self._activate(prev_ctx, thread_id)

    def _activate(self, prev_ctx=None, thread_id=None):
        """set self as the current context"""
        # activate this context if different from the previous context
        thread_id = thread_id if thread_id is not None else PyThread_get_thread_ident()
        if prev_ctx is None:
            try:
                prev_ctx = _current_contexts[thread_id]
            except KeyError:
                pass

        _current_contexts[thread_id] = self
        return Cookie(thread_id, prev_ctx)

    def _deactivate(self, cookie):
        """re-set the previous context as the current context"""
        _current_contexts[cookie.thread_id] = cookie.prev_context

    def _get_node_value(self, node, calling_node=None, prev_ctx=None, thread_id=None):
        alt_ctx = cython.declare(MDFContext)

        # activate the context
        cookie = self._activate(prev_ctx, thread_id)
        prev_ctx = cookie.prev_context

        # if we're in the middle of a node evaluation get
        # the last node on the eval stack
        if calling_node is None:
            calling_node = self._get_calling_node(prev_ctx)

        try:
            # push this node on the stack and get its value
            cqueue_push(self._node_eval_stack, node)
            try:
                return node.get_value(self, thread_id)
            finally:
                cqueue_pop(self._node_eval_stack)

                if node._has_set_date_callback:
                    self._nodes_requiring_set_date_callback[node] = None
                    self._has_nodes_requiring_set_date_callback = True

                # get the context this valuation actually corresponds to
                # (this could be something other than self if self is
                #  shifted and this node doesn't depend on the shift)
                alt_ctx = node.get_alt_context(self)

                # add this node to the calling node's dependencies in the alt context
                if calling_node is not None:
                    calling_node._add_dependency(prev_ctx, node, alt_ctx)

                # if this node can be updated incrementally add it to the set
                # for this context to evaluate when the date's changed
                if node._has_timestep_update:
                    alt_ctx._incrementally_updated_nodes[node] = None
                    alt_ctx._has_incrementally_updated_nodes = True
        finally:
            # deactivate the context
            self._deactivate(cookie)

    def get_value(self, node):
        """
        returns the value of the node in this context
        """
        if _profiling_enabled:
            stop_time = time.clock()
            ctx = cython.declare(MDFContext)
            ctx = self._parent or self
            timer = cython.declare(Timer)
            timer = None
            if len(ctx._timer_stack) > 0:
                timer = ctx._pause_current_timer(stop_time)

        assert isinstance(node, MDFNode), "Attempted to get value of a non-node object"
        
        try:
            return self._get_node_value(node)
        finally:
            if _profiling_enabled and timer is not None:
                timer.resume()

    def set_value(self, node, value):
        """        
        Sets a value of a node in the context.
        """
        cookie = self._activate()
        try:
            # shifted contexts are immutable, with the exception of the now
            # node if the context is shifted by now
            if self._finalized \
            and self._shift_set \
            and not (node is _now_node and node in self._shift_set):
                raise AttributeError("Shifted contexts are read-only")

            # unwrap if necessary
            if isinstance(value, NowNodeValue):
                tmp = cython.declare(NowNodeValue)
                tmp = value
                value = tmp.value

            node.set_value(self, value)
        finally:
            self._deactivate(cookie)

    def set_override(self, node, override_node):
        """
        Sets an override for a node in this context.
        """
        cookie = self._activate()
        try:
            if self._finalized \
            and self._shift_set:
                raise AttributeError("Shifted contexts are read-only")
            node.set_override(self, override_node)
        finally:
            self._deactivate(cookie)

    def __getitem__(self, node):
        """
        Gets a node value in the context.

        See :py:meth:`get_value`.
        """
        return self.get_value(node)

    def __setitem__(self, node, value):
        """
        Sets a node value in the context.
        If ``value`` is an ``MDFNode`` it is applied as an override.

        See :py:meth:`set_value` and :py:meth:`set_override`.
        """
        if isinstance(value, MDFNode):
            self.set_override(node, value)
            return
        self.set_value(node, value)

    def _get_calling_node(self, prev_ctx=None):
        if prev_ctx is None:
            thread_id = PyThread_get_thread_ident()
            try:
                prev_ctx = _current_contexts[thread_id]
            except KeyError:
                prev_ctx = None

            if prev_ctx is None:
                prev_ctx = self

        if len(prev_ctx._node_eval_stack) > 0:
            return prev_ctx._node_eval_stack[-1]

        return None

    def _start_timer(self, node_or_builder):
        """starts the timer for a node and makes that timer the current one"""
        # there's one timer stack on the parent ctx, but each
        # ctx has its own set of timers.
        ctx = self._parent or self
        timer = cython.declare(Timer)
        timer = self._timers.get(node_or_builder)
        if timer is None:
            timer = Timer(node_or_builder)
            self._timers[node_or_builder] = timer
        ctx._timer_stack.append(timer)
        timer.start()
        return timer

    def _stop_timer(self):
        """stops the current timer, pops it off the stack of timers and returns it"""
        stop_time = time.clock()
        ctx = self._parent or self
        timer = cython.declare(Timer)
        timer = ctx._timer_stack.pop()
        timer.stop(stop_time)
        return timer

    def _pause_current_timer(self, stop_time):
        """stops the current timer and returns it, call timer.resume to resume"""
        stop_time = stop_time or time.clock()
        ctx = self._parent or self
        timer = cython.declare(Timer)
        timer = ctx._timer_stack[-1]
        if timer.is_running:
            timer.stop(stop_time)
        return timer

    def _profile(self, node):
        """
        returns an object using with semantics for timing a node evaluation.
        This is called from node sub-classes to indicate the node is doing work.
        """
        if _profiling_enabled:
            return NodeOrBuilderTimer(self, node)
        return _null_timer

    def _profile_builder(self, builder):
        """used by mdf.run"""
        if _profiling_enabled:
            return NodeOrBuilderTimer(self, builder)
        return _null_timer

    def all_nodes(self):
        """
        returns a set of all nodes that have been called in this context
        """
        nodes_with_value = set()
        for node in _all_nodes.itervalues():
            if node.has_value(self) or node.was_called(self):
                nodes_with_value.add(node)
        return nodes_with_value

    def visit_nodes(self, visitor, root_nodes=None, categories=None):
        """
        Calls the visitor for each node and context pair this context knows
        about.

        :return: False if the visitor earlied out, True otherwise.

        :param vistor: function to be called for each node.

        :param root_nodes: if not None root_nodes is a a list of nodes, and only
                           those nodes and any dependent nodes will be visited.

        :param categories: if not not None only nodes belonging to those categories
                           will be visited.

        The visitor function should be of the form:
        
            def visitor(node, context):
                return True or False

        It should return False to indicate the routine should early-out,
        or True otherwise.
        """
        # list of (node, ctx)
        dependencies = []

        if root_nodes is not None:
            for node in root_nodes:
                dependencies.append((node, self))
        else:
            for node in self.all_nodes():
                dependencies.append((node, self))

        if categories is not None:
            categories = set(categories)

        seen = set()
        while dependencies:
            # get the next node from the dependencies list and skip if it's
            # been seen already
            node, ctx = dependencies.pop(0)
            if (node, ctx) in seen:
                continue
            seen.add((node, ctx))

            # if no categories were specified or this node is in one of
            # those categories call visitor
            if categories is None \
            or categories.intersection(node.categories):
                # if the visitor doesn't return True early-out (ignore
                # None as probably most people that use this won't return
                # anything).
                result = visitor(node, ctx)
                if not result and result is not None:
                    return False
    
            # add any dependencies of this node
            deps = node.get_dependencies(ctx)
            for node, ctx in deps:
                dependencies.append((node, ctx))

        # return True to indicate the function didn't early-out
        return True

    def ppstats(self):
        """
        print out some profiling stats
        """
        if not _profiling_enabled:
            print ("*** MDF profiling not enabled ***\n" +
                   "Use the --mdf-profile command line option " +
                   "to enable profiling")
            return

        ctx = cython.declare(MDFContext)

        nodes_without_value = set(_all_nodes.itervalues())
        nodes_with_value = set()

        all_timers = {}
        num_shifts = len(self.get_shifted_contexts())

        for ctx in itertools.chain([self], self.get_shifted_contexts()):
            for node in list(nodes_without_value):
                if node.has_value(ctx):
                    nodes_with_value.add(node)
                    nodes_without_value.remove(node)

            for obj, timer in ctx._timers.iteritems():
                if isinstance(obj, MDFNodeBase):
                    name = obj.name
                elif hasattr(obj, "__name__"):
                    name = "<%s 0x%x>" % (obj.__name__, id(obj))
                elif hasattr(obj, "__class__"):
                    name = "<%s 0x%x>" % (obj.__class__.__name__, id(obj))
                else:
                    name = str(obj)

                all_timers.setdefault(name, []).append(timer)

        def timers_total_time(x):
            _, timers = x
            return sum([t.total_time for t in timers])

        all_timers = all_timers.items()
        all_timers.sort(key=timers_total_time)

        total_time = 0.0
        for name, timers in all_timers:
            node_num_calls = sum([t.num_calls for t in timers])
            node_total_time = sum([t.total_time for t in timers])
            print name
            print "    Num Calls: %d" % node_num_calls
            print "    Total Time: %f" % node_total_time
            print
            total_time += node_total_time

        print "Number of nodes: %s" % len(nodes_with_value)
        print "Number of shifted contexts: %s" % num_shifts
        print "Total Time: %f" % total_time

    def to_dot(self,
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
        from to_dot import _to_dot
        colors = dict(colors)
        colors.update(self._dot_colors)
        return _to_dot(self, filename, nodes, colors, all_contexts, max_depth, rankdir)

def _get_current_context(thread_id=None):
    """returns the current context during node evaluation"""
    if thread_id is None:
        thread_id = PyThread_get_thread_ident()
    try:
        ctx = _current_contexts[thread_id]
    except KeyError:
        ctx = None

    if ctx is None:
        raise NoCurrentContextError()
    return ctx

def _get_context(ctx_id, ctx=None):
    """
    looks up a context by id

    the ctx_id must be related to the current context, or ctx
    if ctx is not None.
    """
    if ctx is None:
        ctx = _get_current_context()

    parent = ctx._parent if ctx._parent is not None else ctx
    if parent._id_obj == ctx_id:
        return parent

    return parent._all_child_contexts[ctx_id]

def shift(node, target=None, values=None, shift_sets=None):
    """
    This function is for use inside node functions.
    
    Applies shifts to the current context for each
    shift specified and returns the value of 'node'
    with each of the shifts applied.

    If target and values are specified 'target' is a node
    to apply a series of shifts to, specified by 'values'.
    
    If shifts_sets is specified, 'shift_sets' is a list
    of nodes to values dictionaries, each one specifying
    a shift.

    If the same shift set dictionaries are used several times
    `ShiftSet` objects may be used instead which will
    be slightly faster. See :py:func:`make_shift_set`.

    Returns a list of the results of evaluating node for
    each of the shifted contexts in the same order
    as values or shift_sets.

    See :py:meth:`MDFContext.shift` for more details about shifted
    contexts.
    """
    if _profiling_enabled:
        stop_time = time.clock()

    thread_id = PyThread_get_thread_ident()
    ctx = _get_current_context(thread_id)

    if _profiling_enabled:
        timer = ctx._pause_current_timer(stop_time)

    shifted_ctx = cython.declare(MDFContext)
    results = cython.declare(list)

    # get the calling node now to avoid _get_node_value having to get it each time
    calling_node = ctx._get_calling_node(ctx)

    try:
        if shift_sets is not None:
            results = []
            for shift_set in shift_sets:
                shifted_ctx = ctx._shift(shift_set)
                results.append(shifted_ctx._get_node_value(node,
                                                           calling_node,
                                                           ctx,
                                                           thread_id))
        else:
            results = []
            for value in values:
                shifted_ctx = ctx._shift({target : value})
                results.append(shifted_ctx._get_node_value(node,
                                                           calling_node,
                                                           ctx,
                                                           thread_id))
        return results
    finally:
        if _profiling_enabled:
            timer.resume()

def _shift(node, target, values, **kwargs):
    """
    deprecated; see shift
    """
    warnings.warn("_shift is deprecated; use shift instead", DeprecationWarning)
    return shift(node, target, values)

def make_shift_set(shift_set_dict):
    """
    Return a 'ShiftSet' object that encapsulates the information
    required to get a shifted context.
    
    This can be used to pass to the :py:func:`shift` function instead
    of a dictionary for better performance when regularly shifting by
    the same thing.
    """
    return ShiftSet(shift_set_dict)

def get_nodes(category=None):
    """
    returns a list of nodes.
    
    ``category`` maybe a string or a list of strings. If specified,
    only nodes matching those categories will be returned.
    """
    if category is None:
        return _all_nodes.values()

    if isinstance(category, basestring):
        category = [category]
    categories = set(category)

    # get all nodes matching categories    
    nodes = []
    for node in _all_nodes.itervalues():
        if categories.intersection(node.categories):
            nodes.append(node)

    return nodes
