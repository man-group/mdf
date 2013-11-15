from nodes cimport NodeState, MDFNode, MDFVarNode
from context cimport MDFContext, _all_nodes

cpdef _pickle_context(MDFContext ctx)
cpdef MDFContext _unpickle_context(cls, ctx_id, now, node_states, shift_sets)

cpdef _pickle_node(MDFNode node)
cpdef MDFNode _unpickle_node(node_name, modulename, is_bound, vardata=?)

cpdef _pickle_custom_node(MDFNode node,
                          MDFNode base_node,
                          base_node_method_name,
                          kwargs)

cpdef _unpickle_custom_node(node_name,
                            modulename,
                            is_bound,
                            base_node,
                            base_node_method_name,
                            kwargs)

cpdef _pickle_shift_set(shift_set)
cpdef _unpickle_shift_set(shift_set_dict)
 