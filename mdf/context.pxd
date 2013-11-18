"""
Cython optimizations for context.py
"""
from cqueue cimport *

cdef extern from "Python.h":
    long PyThread_get_thread_ident()

# forward declarations
cdef class MDFContext
cdef class MDFNodeBase
cdef class ShiftSet

# globals
cdef:
    dict _current_contexts
    dict _all_nodes
    int _profiling_enabled

ctypedef fused ShiftSetOrDict:
    ShiftSet
    dict

cdef inline tuple _make_shift_key(ShiftSetOrDict shift_set)

cdef class ShiftSet(dict):
    cdef dict _shift_keys
    cdef tuple _get_shift_key(self, MDFContext context)

cdef class Timer(object):
    cdef object node_or_builder
    cdef double started_time
    cdef public int num_calls
    cdef public double total_time
    cdef public int is_running

    cdef inline start(self)
    cdef inline stop(self, double stop_time)
    cdef inline resume(self)

cdef class NodeOrBuilderTimer(object):
    cdef MDFContext ctx
    cdef object node_or_builder

    cpdef __enter__(self)
    cpdef __exit__(self, exc_type, exc_value, traceback)

cpdef int _profiling_is_enabled()

cdef class Cookie(object):
    cdef object thread_id
    cdef MDFContext prev_context

cdef class NowNodeValue(object):
    cdef object value

cdef class MDFNodeBase(object):
    cdef bint _has_set_date_callback
    cdef bint _has_timestep_update

    #
    # subset of MDFNode C methods used by MDFContext
    #
    cdef MDFContext get_alt_context(self, MDFContext ctx)
    cdef _add_dependency(self, MDFContext ctx, MDFNodeBase called_node, MDFContext called_ctx)
    cdef get_value(self, MDFContext ctx, thread_id=?)

    #
    # subset of MDFNode API methods used by MDFContext
    #
    cpdef get_dependencies(self, MDFContext ctx)
    cpdef int has_value(self, MDFContext ctx)
    cpdef set_dirty(self, MDFContext ctx, int flags=?)
    cpdef clear(self, MDFContext ctx)
    cpdef clear_value(self, MDFContext ctx)
    cpdef set_value(self, MDFContext ctx, value)
    cpdef set_override(self, MDFContext ctx, MDFNodeBase override_node)

cdef class MDFContext(object):
    # the two ids are the same, but the object one is only available in c
    # and is an object because it's faster to use that when doing a dict lookup
    cdef public int _id
    cdef object _id_obj

    cdef int _finalized
    cdef public object _now

    cdef MDFContext _parent
    cdef dict _all_child_contexts
    cdef cqueue _node_eval_stack
    cdef dict _shifted_cache
    cdef dict _is_shift_of_cache
    cdef object _shifted_contexts
    cdef dict _shift_set
    cdef tuple _shift_key
    cdef object _shift_key_set
    cdef dict _timers
    cdef object _timer_stack

    # updated by MDFNode.get_value
    cdef dict _incrementally_updated_nodes
    cdef int _has_incrementally_updated_nodes
    cdef dict _nodes_requiring_set_date_callback
    cdef int _has_nodes_requiring_set_date_callback

    cdef _init(self, now,
               MDFContext _shift_parent=?,
               _shift_set=?,
               _cache_shifted=?)

    #
    # internal C only methods
    #
    cdef Timer _start_timer(self, object node)
    cdef Timer _stop_timer(self)
    cdef MDFNodeBase _get_calling_node(self, MDFContext prev_ctx=?)
    cdef MDFContext _shift(MDFContext self, shift_set, int cache_context=?)
    cdef Cookie _activate(self, MDFContext prev_ctx=?, thread_id=?)
    cdef _deactivate(self, Cookie cookie)
    cdef _set_date(self, date)

    # 
    # semi-public C methods used by MDFNode
    #
    cdef _get_node_value(self, MDFNodeBase node, MDFNodeBase calling_node=?, MDFContext prev_ctx=?, thread_id=?)
    cdef Timer _pause_current_timer(self, double stop_time)
    cdef object _profile(self, node)
    cpdef object _profile_builder(self, builder)

    #
    # public API functions
    #
    cpdef _activate_ctx(self, MDFContext prev_ctx=?, thread_id=?)
    cpdef get_value(self, MDFNodeBase node)
    cpdef set_value(self, MDFNodeBase node, value)
    cpdef set_override(self, MDFNodeBase node, MDFNodeBase override_node)
    cpdef MDFContext get_parent(self)
    cpdef dict get_shift_set(self)
    cpdef list get_shifted_contexts(self)
    cpdef iter_shifted_contexts(self)    
    cpdef set_date(self, date)
    cpdef get_date(self)
    cpdef shift(self, shift_set, cache_context=?)
    cpdef ppstats(self)
    cpdef clear(self)
    cpdef is_shift_of(self, MDFContext other)
    cpdef to_dot(self, filename=?, nodes=?, colors=?, all_contexts=?, max_depth=?, rankdir=?)

cpdef shift(MDFNodeBase node, MDFNodeBase target=?, values=?, shift_sets=?)
cpdef MDFContext _get_current_context(thread_id=?)
cpdef MDFContext _get_context(ctx_id, MDFContext ctx=?)
cpdef get_nodes(category=?)
cpdef make_shift_set(dict shift_set_dict)
cdef public MDFNodeBase _now_node
