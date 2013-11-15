from nodes cimport MDFNode, MDFEvalNode, MDFIterator
from context cimport MDFContext

cdef class MDFCustomNode(MDFEvalNode):
    cdef MDFNode _base_node
    cdef object _base_node_method_name
    cdef object _node_type_func
    cdef object _cn_func
    cdef object _category
    cdef int _call_with_filter_node
    cdef int _call_with_filter
    cdef int _call_with_self
    cdef int _call_with_no_value
    cdef dict _kwargs
    cdef dict _kwnodes
    cdef dict _kwfuncs

    # internal C methods
    cdef inline dict _get_kwargs(self)
    cdef _get_nodetype_func_kwargs(self, int remove_special=?)

    # protected python methods
    cpdef _cn_eval_func(self)

cdef class MDFCustomNodeIterator(MDFIterator):
    cdef MDFCustomNode custom_node
    cdef object func
    cdef object node_type_func
    cdef object value_generator
    cdef int is_generator
    cdef int node_type_is_generator
    cdef object node_type_generator

cdef class MDFQueueNode(MDFCustomNode):
    pass

cdef class _queuenode(MDFIterator):
    cdef object queue
    cdef int as_list

    cpdef next(self)
    cpdef send(self, value)

cdef class MDFDelayNode(MDFCustomNode):
    cdef object _dn_func
    cdef dict _dn_per_ctx_data
    cdef int _dn_is_generator
    cdef int _dn_lazy
    
    cpdef _dn_get_prev_value(self)

cdef class _delaynode(MDFIterator):
    cdef int lazy
    cdef int skip_nans
    cdef object queue

    cpdef next(self)
    cpdef send(self, value)

cdef class _samplenode(MDFIterator):
    cdef object _offset
    cdef MDFNode _date_node
    cdef object _sample

    cpdef next(self)
    cpdef send(self, value)

cdef class MDFNanSumNode(MDFCustomNode):
    pass

cdef class _nansumnode(MDFIterator):
    cdef object accum
    cdef double accum_f
    cdef int is_float

    cpdef next(self)
    cpdef send(self, value)

    cdef inline _send_vector(self, value)
    cdef inline double _send_float(self, double value)

cdef class MDFCumulativeProductNode(MDFCustomNode):
    pass
    
cdef class _cumprodnode(MDFIterator):
    cdef object accum
    cdef double accum_f
    cdef object nan_mask
    cdef int nan_mask_f
    cdef int is_float
    cdef int skipna

    cpdef next(self)
    cpdef send(self, value)

    cdef inline _send_vector(self, value)
    cdef inline double _send_float(self, double value)

cdef class _ffillnode(MDFIterator):
    cdef int is_float
    cdef double current_value_f
    cdef object current_value
    
    cpdef next(self)
    cpdef send(self, value)

cdef class _returnsnode(MDFIterator):
    cdef int is_float
    cdef double current_value_f
    cdef double prev_value_f
    cdef double return_f
    cdef object current_value
    cdef object prev_value
    cdef object returns

    cpdef next(self)
    cpdef send(self, value)

cdef class _rowiternode(MDFIterator):
    cdef object _data
    cdef MDFNode _index_node
    cdef object _index_node_type
    cdef object _iter
    cdef object _current_index
    cdef object _current_value
    cdef object _prev_value
    cdef object _missing_value_orig
    cdef object _missing_value
    cdef int _ffill
    cdef int _is_dataframe
    cdef int _is_widepanel
    cdef int _is_series
    cdef int _index_to_date

    cdef _set_data(self, data)
    cdef _next_dataframe(self)
    cdef _next_widepanel(self)
    cdef _next_series(self)

    cpdef next(self)
    cpdef send(self, value)

cdef class MDFLookAheadNode(MDFCustomNode):
    cpdef on_set_date(self, MDFContext ctx, date)

cpdef _lookaheadnode(value_unused, MDFLookAheadNode owner_node, periods, MDFNode filter_node=?, offset=?)
