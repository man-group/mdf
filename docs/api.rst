.. py:module:: mdf

API Reference
=============

* :ref:`node_types`
    * :py:func:`varnode`
    * :py:func:`evalnode`
    * :py:func:`queuenode`
    * :py:func:`nansumnode`
    * :py:func:`cumprodnode`
    * :py:func:`ffillnode`
    * :py:func:`rowiternode`
    * :py:func:`returnsnode`
    * :py:func:`lookaheadnode`
    * :py:func:`applynode`
* :ref:`node_factories`
    * :py:func:`datanode`
    * :py:func:`filternode`
* :ref:`custom_node_types`
    * :py:func:`nodetype`
* :ref:`pre_defined_nodes`
    * :py:func:`now`
* :ref:`mdffunctions`
    * :py:func:`shift`
    * :py:func:`run`
    * :py:func:`plot`
    * :py:func:`build_dataframe`
    * :py:func:`get_final_values`
    * :py:func:`scenario`
    * :py:func:`plot_surface`
    * :py:func:`make_shift_set`
* :ref:`mdfclasses`
    * :py:class:`MDFContext`
    * :py:class:`MDFNode`
    * :py:class:`MDFEvalNode`
    * :py:class:`CSVWriter`
    * :py:class:`DataFrameBuilder`
    * :py:class:`FinalValueCollector`

 .. _node_types:

Nodes Types
-----------

.. autofunction:: varnode([name] [, default] [, category])

.. autofunction:: evalnode(func [, filter] [, category])

.. autofunction:: queuenode(func [, size] [, filter] [, category])

.. autofunction:: delaynode(func [, periods] [, initial_value] [, lazy] [, filter] [, category])

.. autofunction:: nansumnode(func [, filter] [, category])

.. autofunction:: cumprodnode(func [, filter] [, category])

.. autofunction:: ffillnode(func [, initial_value])

.. autofunction:: rowiternode(func [, index_node=now] [, missing_value=np.nan] [, filter] [, category])

.. autofunction:: returnsnode(func [, filter] [, category])

.. autofunction:: applynode(func, [, args=()] [, kwargs={}] [, category])

.. autofunction:: lookaheadnode(func, periods [, offset=pa.datetools.BDay()] [, filter] [, category])

.. _node_factories:

Node Factory Functions
----------------------

.. autofunction:: datanode([name=None,] data [, index_node] [, missing_value] [, delay] [, name] [,filter] [,category])

.. autofunction:: filternode([name=None,] data [, index_node] [, delay] [, name] [,filter] [,category])

.. _custom_node_types:

Custom Node Types
-----------------

.. autofunction:: nodetype(func)

.. _pre_defined_nodes:

Pre-defined Nodes
-----------------

.. py:function:: now()

    Pre-defined node present in every context that always evaluates to the date
    set on the context.
    
    See :py:meth:`MDFContext.get_date` and :py:meth:`MDFContext.set_date`.

.. _mdffunctions:

Functions
---------

.. autofunction:: shift(node, target [, values] [, shift_sets])

.. autofunction:: run(date_range [, callbacks=[]] [, values={}] [, shifts=None] [, filter=None] [, ctx=None])

.. autofunction:: plot(date_range, nodes [, labels=None] [, values={}] [, filter=None] [, ctx=None])

.. autofunction:: build_dataframe(date_range, nodes [, labels=None] [, values={}] [, filter=None] [, ctx=None])

.. autofunction:: get_final_values(date_range, nodes [, labels=None] [, values={}] [, filter=None] [, ctx=None])

.. autofunction:: scenario(date_range, result_node, x_node, x_shifts, y_node, y_shifts [, values={}] [, filter=None] [, ctx=None] [, dtype=float])

.. autofunction:: plot_surface(date_range, result_node, x_node, x_shifts, y_node, y_shifts [, values={}] [, filter=None] [, ctx=None] [, dtype=float])

.. autofunction:: make_shift_set(shift_set_dict)

.. _mdfclasses:

Classes
-------

MDFContext
~~~~~~~~~~

.. autoclass:: MDFContext

    .. automethod:: __init__(now)
    
    .. automethod:: save(filename, bat_filename=None, start_date=None, end_date=None)

    .. automethod:: load(filename)

    .. automethod:: get_date()

    .. automethod:: set_date(date)

    .. automethod:: get_value(node)

    .. automethod:: set_value(node, value)
    
    .. automethod:: set_override(node, value)

    .. automethod:: __getitem__(node)
    
    .. automethod:: __setitem__(node, value)

    .. automethod:: shift(shift_set, cache_context=True)
    
    .. automethod:: to_dot(filename=None, nodes=None, colors={}, all_contexts=True, max_depth=None, rankdir="LR")

MDFNode
~~~~~~~

.. py:class:: MDFNode

    Nodes should be viewed as opaque objects and not instanciated through
    anything other than the decorators provided.
    
    They are callable objects and should be called from inside other
    node functions.
    
    When called they are evaluated in the current context and value is
    returned. If called multiple times a cached value is returned unless
    the node has been marked as requiring re-evaluation by one of its
    depenedencies changing.

MDFEvalNode
~~~~~~~~~~~

.. py:class:: MDFEvalNode

    Sub-class of :py:class:`MDFNode` for nodes that are evaluated rather
    than plain value storing nodes.
    
    This is an opaque type and shouldn't be used to construct nodes.
    Instead use the node type decorators.

CSVWriter
~~~~~~~~~

.. autoclass:: CSVWriter

    .. automethod:: __init__(fh, nodes [, columns=None])


DataFrameBuilder
~~~~~~~~~~~~~~~~

.. autoclass:: DataFrameBuilder

    .. automethod:: __init__(nodes [, labels=None])
    
    .. automethod:: clear()
    
    .. automethod:: get_dataframe([ctx=None])
    
    .. autoattribute:: dataframes

    .. autoattribute:: dataframe
    
    .. automethod:: plot([show=True])

FinalValueCollector
~~~~~~~~~~~~~~~~~~~

.. autoclass:: FinalValueCollector

    .. automethod:: __init__(nodes)
    
    .. automethod:: clear()
    
    .. automethod:: get_values([ctx=None])
    
    .. automethod:: get_dict([ctx=None])
    
    .. autoattribute:: values
