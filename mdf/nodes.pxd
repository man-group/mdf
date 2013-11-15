from context cimport MDFContext, MDFNodeBase
from context cimport _get_current_context, _get_context, _profiling_enabled
from cqueue cimport *

cdef int DIRTY_FLAGS_NONE
cdef int DIRTY_FLAGS_ALL
cdef int DIRTY_FLAGS_TIME

cdef class NodeState(object):
    cdef object ctx_id
    cdef int dirty_flags
    cdef int has_value
    cdef object date
    cdef object value
    cdef MDFContext alt_context
    cdef object override
    cdef object override_cache
    cdef cqueue set_dirty_queue

    cdef dict callers
    cdef dict callees

    cdef dict depends_on_cache
    cdef object add_dependency_cache

    cdef int ctx_set_date_callback_done
    cdef int ctx_incremental_update_callback_done

    cdef int called
    cdef MDFContext prev_alt_context
    cdef object generator

cdef class MDFIterator(object):
    cpdef next(self)

cdef class MDFNode(MDFNodeBase):
    # from base class
    # cdef bint _has_set_date_callback
    # cdef bint _has_timestep_update

    cdef object _name
    cdef object _short_name
    cdef int _is_bound
    cdef object _modulename
    cdef dict _states
    cdef tuple _categories
    cdef int _has_on_dirty_callback
    cdef int _dirty_flags_propagate_mask
    cdef public dict _derived_nodes

    #
    # C API
    #
    cdef NodeState _get_state(self, MDFContext ctx)
    cdef _clear_dependency_cache(self, MDFContext ctx)
    cdef _depends_on(self, NodeState node_state, MDFNode other, other_ctx_id)
    cdef _set_dirty(self, NodeState node_state, int flags, int _depth)
    cdef _touch(self, NodeState node_state, int flags=?, int _quiet=?, int _depth=?)
    cdef _get_cached_value_and_date(self, MDFContext ctx, NodeState node_state)
    cdef MDFContext _get_alt_context(self, MDFContext ctx)
    cdef _get_value(self, MDFContext ctx, NodeState node_state)
    cdef _set_value(self, MDFContext ctx, NodeState node_state, value, int _quiet=?)
    cdef MDFNode _get_override(self, MDFContext ctx, NodeState node_state)

    # overriden from base class
    cdef MDFContext get_alt_context(self, MDFContext ctx)
    cdef _add_dependency(self, MDFContext ctx, MDFNodeBase called_node, MDFContext called_ctx)
    cdef get_value(self, MDFContext ctx, thread_id=?)

    # semi-public API for MDFContext to use
    cpdef _get_cached_value(self, MDFContext ctx)
    cpdef _reset_alt_context(self, MDFContext ctx)

    #
    # public python API
    #
    cpdef add_dependency(self, MDFContext ctx, MDFNodeBase called_node, MDFContext called_ctx)
    cpdef get_dependencies(self, MDFContext ctx)
    cpdef get_callers(self, MDFContext ctx)
    cpdef clear(self, MDFContext ctx)
    cpdef clear_value(self, MDFContext ctx)
    cpdef depends_on(self, MDFContext ctx, MDFNodeBase other_node, MDFContext other_ctx)
    cpdef int is_dirty(self, MDFContext ctx, int mask=?)
    cpdef set_dirty(self, MDFContext ctx, int flags=?)
    cpdef touch(self, MDFContext ctx, int flags=?)
    cpdef int has_value(self, MDFContext ctx)
    cpdef int was_called(self, MDFContext ctx)
    cpdef set_value(self, MDFContext ctx, value)
    cpdef set_override(self, MDFContext ctx, MDFNodeBase override_node)
    cpdef MDFNode get_override(self, MDFContext ctx)
    cpdef get_state(self, MDFContext)

cdef class MDFVarNode(MDFNode):
    cdef object _default_value
    cdef MDFContext _get_alt_context(self, MDFContext ctx)
    cdef _get_value(self, MDFContext ctx, NodeState node_state)

cdef class MDFEvalNode(MDFNode):
    cdef object _func
    cdef int _is_generator
    cdef object _filter_func
    cdef dict _bound_nodes

    # docstring of the inner function
    cdef public object func_doc

    # C API
    cdef _get_value(self, MDFContext ctx, NodeState node_state)
    cdef MDFContext _get_alt_context(self, MDFContext ctx)
    cdef _set_value(self, MDFContext ctx, NodeState node_state, value, int _quiet=?)
    cdef _fixup_alt_context(self, MDFContext ctx, NodeState node_state, MDFContext alt_ctx)

    # protected Python API
    cpdef _bind(self, MDFEvalNode other, owner)
    cpdef _bind_function(self, func, owner)
    cpdef _get_func_name(self, func)
    cpdef _validate_func(self, func)
    cpdef _set_func(self, func)

    # public Python API
    cpdef clear(self, MDFContext ctx)
    cpdef get_filter(self)
    cpdef set_value(self, MDFContext ctx, value)

cdef class MDFTimeNode(MDFVarNode):
    # C API
    cdef MDFContext _get_alt_context(self, MDFContext ctx)
    cdef _touch(self, NodeState node_state, int flags=?, int _quiet=?, int _depth=?)

    # public Python API
    cpdef set_value(self, MDFContext ctx, value)

