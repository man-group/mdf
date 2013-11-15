.. py:currentmodule:: mdf

######################
Implementation Details
######################

The section of the documentation is an overview of the internal design and implementation
of MDF. It should not be necessary to read or understand this section in order to use MDF.

************
Building MDF
************

Use of Cython
=============

Cython is used extensively throughout MDF. It is used in such a way, however, that it's
still possible to call MDF code *without* compiling it to allow easier debugging of
the MDF internals.

All of the implementation code is in plain ``.py`` files with additional type information
given using :py:func:`cython.declare` for local variables and ``.pxd`` files for
class and module attribute type information. The modules are compiled into separate
extensions, and type information is shared between the compilation units via the
``.pxd`` files.

There is significant overhead calling a Python function compared with calling a C
function. Even when Cythoned methods are Cythoned using the ``cpdef`` keyword so
they can be called both from Python or from C there is still additional overhead
when the method is called compared with calling a plain C function. This is why
the Cythoned classed have some ``cdef`` private methods with corresponding ``cpdef``
public methods. Internally the ``cdef`` methods should always be called unless
it's expected that the method may be overridden by a Python subclass.

Whenever making changes to MDF the code must be profiled before and after as
what look like small changes can drastically change the performance due to
the number of times some of the internal functions and methods get called.

Compiling MDF
=============

Building MDF requires Cython version 0.16 or later to be installed.

As with any Cythoned package it also requires a distutils compatible C compiler
(e.g. Visual Studio 2008 for Windows or GCC for linux).

MDF can be compiled *in-place* to allow you to test changes and use your local
check out of MDF in the usual Python way using setup.py::

    python setup.py build_ext --inplace

Or to build the egg::

    python setup.py bdist_egg

Current setuptools and Cython (0.16) doesn't pick up changes to ``.pxd`` files
correctly when determining dependencies, and so changes to ``.pxd`` files won't
trigger a recompilation of all affected object files. If this happens the
easiest work around is to simply delete the generated ``.c`` files that will
be in the same folder as their corresponding ``.py`` files.

Running/Debugging Non-Cythoned MDF
==================================

To be able to import the MDF modules without first compiling you need to have
Cython installed. Cython is not required to import the modules after compilation.

There are various places where it has not been possible to have the pure-python
version of the code identical to the Cythonized code. These cases are clearly
commented, and in order to use MDF without compiling it you need to first
un-comment these bits of code.

Follow the instructions in the comments regarding Cython in the following
files:

- nodes.py
- context.py
- cqueue.py
- ctx_pickle.py

Although you will be able to use MDF in its uncompiled state for debugging
its internals you will find it will be orders of magnitude slower than
the compiled version.

Debugging the Compiled Cython Code
==================================

In setup.py there is a module variable ``cdebug``. Set this to ``True`` to enable the
compiler and linker flags to build debug versions of the extensions. This only
sets the Visual Studio debug flags, and for GCC you should change them to whatever
flags you required (usually ``-g`` is sufficient).

Once you have rebuilt the extensions with the debug flags set you can now attach
a debugger to a running python instance in order to debug the cythoned MDF functions.

Profiling MDF
=============

MDF includes its own counters and timers for profiling code run using MDF. This
should be used to identify hotspots in user code. See :py:meth:`MDFContext.ppstats`. 

If necessary further profiling can be done using cPython. A visual profiler such
as RunSnakeRun or kCacheGrind can be useful for understanding the cProfiler
output. kCacheGrind provides more detail than RunSnakeRun but requires the
cProfile output to be converted to a calltree using the `pyprof2calltree` pacakge.

Profiling MDF Internals
-----------------------

cProfile can also be used to profile the MDF internal Cythoned functions and methods.
In setup.py there is a module variable ``cython_profile``. Set this to ``True`` to
enable profiling of Cythoned code.

Once you have rebuilt the extensions with the profile flag set you can now use
cProfile to profile an application using MDF with the time spent in Cythoned
functions included.

The addition of the profiling code to every Cythoned function adds significant
overhead as many of the functions have been optimised to be very lightweight
and may be inlined by the compiler. Adding the profiling code bloats these functions
and distorts the running time significantly and it can therefore be very hard
to determine accurate timings and hotspots using this method.

It is better to use a non-invasive statistical profiler such as Intel VTune or
Sleepy (see 'Very Sleepy' for Windows). To profile MDF with one of these
profilers you will need to build MDF with debug symbols using the
``cdebug`` setting in setup.py. For more accurate profiling you may want to
build with compiler optimisations as well as debug symbols, in which case remove
``/Od`` from the compiler flags.

Profiling with a statistical profiler requires a little more knowledge and intuition about
how the Python code is translated to C by Cython. The Cython code is annotated with
the original Python code which makes this much easier, and browsing the code around
the functions of interest before looking at the profiling results will make understanding
the results of the profiling simpler. This is by far the best way to get a proper feel
for where time is being spent inside MDF though and it is worth persevering.

*********************
Source Code Overview
*********************

context.py
==========

context.py contains (almost) everything to do with :py:class:`MDFContext`. It also includes
a class :py:class:`MDFNodeBase` from while :py:class:`MDFNode` is derived. It's done this
way with the node base class in context.py so that the context code can call C methods
on the cythoned MDFNode objects without having to cimport ``nodes.pxd`` as that would
result in a circular dependency.

context.py defines the following classes:

- :py:class:`MDFContext`
- :py:class:`MDFNodeBase`
- :py:class:`ShiftSet`

and the public API functions:

- :py:func:`shift`
- :py:func:`get_nodes`
- :py:func:`make_shift_set`

nodes.py
========

nodes.py defines the following classes:

- :py:class:`MDFNode`
- :py:class:`MDFVarNode`
- :py:class:`MDFEvalNode`
- :py:class:`MDFTimeNode`
- :py:class:`MDFIterator`
- :py:class:`MDFIteratorFactory`
- :py:class:`MDFIteratorFactory`
- :py:class:`MDFCallable`

and the public API functions:

- :py:func:`varnode`
- :py:func:`vargroup`
- :py:func:`evalnode`
- :py:func:`now`

These classes are almost always only used internally. The API functions return instances
of the node classes and so it's almost never necessary to refer to any of these classes
outside of MDF. 

:py:class:`NodeState` is the per context state associated with a particular node.
This isn't exposed outside of the Cythoned code and for external use. If it's referenced
at all should be considered an opaque type (hence not appearing in the API reference).

cqueue.py
=========

cqueue is an implementation of a double ended queue, like :py:class:`collections.deque`.
Although it's a queue it's specialised to represent a stack efficiently. Underlying it
is a normal python list, and as items are pushed on it grows as it runs out of space.
It keeps an index to the start and end of the queue and so popping items off either
end simply means moving these indexes.

Because the items aren't reshuffled popping items off the left and adding to the right
would result in more and more memory being allocated, but constantly pushing and popping
the right is much faster than a deque, which is what this container is used for.

This could probably be improved by writing it in plain C rather than using a Python
list, but at the time of writing it was sufficiently faster than :py:class:`collections.deque`
for what it's being used for that it wasn't optimised further.

nodetypes.py
============

nodetypes.py is where the :py:func:`nodetype` decorator is defined, and also all the
various classes that are necessary for implementing more general node types derived
from :py:class:`MDFEvalNode`:

- :py:class:`MDFCustomNode`
- :py:class:`MDFCustomNodeIterator`
- :py:class:`MDFCustomNodeIteratorFactory`
- :py:class:`MDFCustomNodeMethod`
- :py:class:`MDFCustomNodeDecorator`

:py:class:`MDFCustomNode` is derived from :py:class:`MDFEvalNode` and is constructed
with an additional function, iterator or generator that converts the result of the evalnode
to whatever the specific nodetype should return. 

:py:class:`MDFCustomNodeDecorator` is what's returned by the :py:func:`nodetype` decorator,
which is itself a decorator. It converts whatever function (or generator or iterator class)
it decorates into a node type decorator.

All the built-in node type decorators use the :py:func:`nodetype` and subclasses of
:py:class:`MDFIterator` to achieve the various different calculations.

:py:class:`MDFCustomNodeMethod` is what's used to add the methods to all :py:class:`MDFEvalNode`
instances (see :ref:`nodetype_method_syntax`). It's a callable object that when called
creates or fetches a previously created node. The returned node is the node it was called on
wrapped with a node type. 

runner.py
=========

runner.py is where the various functions for running an MDF graph over time and extracting
values, including parallel computation of scenarios.

to_dot.py
=========

to_dot.py implements the :py:meth:`MDFContext.to_dot` method. This uses :py:mod:`pydot`
and ``Graphviz`` to render the graph as an image in a variety of different formats
( .dot, .svg, .png, e.t.c.).

parser.py
=========

Parsing code for use by the magic ipython functions and also for parsing Python code
to find the left hand side of node assignments for defaulting node names, e.g.::

    a_var_node = varnode()

The parser looks at the callstack and parses the line of source code to get ``"a_var_node"``
to use as the name for this node.

ctx_pickle.py
=============

All the pickling code for contexts and nodes is separated out from the main files, and is
implemented as functions in this file. These functions are imported from node.py and
context.py and called from the various ``__reduce__`` methods on the associated classes.

builders sub-package
====================

The builders sub-package is where all the provided callable objects intended to be used
with :py:func:`run` are.

io sub-package
==============

The methods :py:meth:`MDFContext.save` and :py:meth:`MDFContext.load` are implemented
in this sub-package. The serialisation is done via pickling in ctx_pickle.py, but this
sub-package can also read and write compressed files.

pylab sub-package
=================

The pylab sub-package is where all the various magic ipython function are. This should
only be imported for interactive use not from scripts as it depends on IPython. 

regression sub-package
======================

Regression testing is done by evaluating nodes in two different processes started
from different virtualenvs. The code in this package manages starting the processes
in the correct virtualenvs and collecting the values of the nodes over time in
both child processes.

The data collection is done by a specialized builder, :py:class:`DataFrameDiffer`,
but other differ could be written by subclassing :py:class:`Differ`.

The interprocess communication is done using Pyro and the proxy objects from the
remote sub-package.

remote sub-package
==================

remote contains code shared between the various parallel processing components of MDF
for creating subprocesses and the Pryo server and objects used for interprocess
communication.

It also contains custom Pyro serialisation functions that autmatically compress data
larger than a certain size on the fly using bz2. 

tests sub-package
=================

Unit tests.

***************
Node Evaluation
***************

Nodes only have values in a context, so it makes sense that to get a nodes value it's
evaluated by starting with the context. Indexing into the context with a node (see
:py:meth:`MDFContext.__getitem__`) calls :py:meth:`MDFContext.get_value`, which is
a public API method. This calls an internal C method :py:meth:`MDFContext._get_node_value`
which is where the work is actually done.

Once inside a node evaluation to avoid passing the current context around to every node
call to allow the nodes to evaluate other nodes, there's the notion of a currently
active context. The currently active context is stored in a dictionary that maps thread
id to the MDFContext (_current_contexts in context.py). See also :py:func:`_get_current_context`.

Dependencies
============

Although it's common to talk about one node being dependent on another, actually
the dependencies are between a `node in a context` and another `node in a context`.
The contexts need not be the same as one node can call another node in another
context (when shifting, for example).

Dependency tracking is facilitated by :py:meth:`MDFContext._get_node_value` keeping track of
the current node being evaluated (and the context it's being evaluated in). These are kept
in a stack (actually it's a queue - but conceptually it's a stack) so to find the node
that's calling the current node being evaluated it just needs to look at the last item
in the stack. Before dropping into the node evaluation itself the current node and context
are pushed onto the stack.

All dependencies are discovered at runtime by observing which nodes call other nodes. This
can either be directly, or a node may call a function that then calls other nodes.
Before anything is evaluated MDF knows nothing about the dependencies between nodes. 

Each context has its own node evaluation stack and so to find the node and context calling
the current node and context the stack belonging to the previous context is examined.
A restriction is that the same context can't be used from different threads concurrently,
but different contexts can be used in different threads because the current context is effectively
a per thread variable (although it's in a dict rather than TLS).

Once discovering what the calling node and context is (if any), the current node is pushed
onto the current context's node evaluation stack and :py:meth:`MDFNodeBase.get_value` is called
to retrieve or calculate the node value in the context. After the node has been evaluated
the node is popped off the evaluation stack and a dependency is established between the previous
node and context and the current node and context by calling :py:meth:`MDFNodeBase._add_dependency`.

As the dependencies are context dependent the relationships are stored on the :py:class:`NodeState`
object associated with the node and context pair.   

:py:meth:`MDFNodeBase._add_dependency` is written such that re-calling it for the same node and
context is fast, and so it's called everytime :py:meth:`MDFContext._get_node_value` is called
regardless of whether the dependency has been discovered previously or not.
 
Node evaluation
===============

As mentioned above the entry point for evaluating a node is :py:meth:`MDFContext.__getitem__` or
:py:meth:`MDFContext.get_value`, which in turn calls the internal C method
:py:meth:`MDFContext._get_node_value`. This ultimately calls :py:meth:`MDFNodeBase.get_value`,
which each derived node class implements.

Varnodes
--------

varnodes are the simplest type of node. Getting their value just involves looking to see if the
:py:class:`NodeState` has a value for the current context and return that. If there is no
value in the :py:class:`NodeState` the default value for the node is returned if there is one,
or an exception is raised.

Evalnodes
---------

Eval nodes wrap a function, generator or :py:class:`MDFIterator`. To get their value the wrapped
function is called and the result is cached on the :py:class:`NodeState` for the context the node
is being evaluated in.

Once a node has been evaluated once it is marked as not needing to be re-evaluated. If the node
is evaluated again the previous result is returned as long as none of the dependencies of the
node have changed.

Dirty flags
-----------

Whether the node needs evaluating or not is determined by the ``dirty_flags`` on the :py:class:`NodeState`
object for the node in each context. When the node is evaluated these flags are cleared to indicate
the node isn't dirty and the previously cached value can be used. When any node is changed the
:py:meth:`MDFNode.set_dirty` method is called which marks the node as dirty and then marks all the nodes
calling this node as dirty if they are not already flagged dirty. The dirty flags propagate all the
way through the graph for all nodes and context pairs that depend on the node in the context being
dirtied.

Generators and iterators
------------------------

If an evalnode wraps a generator or iterator then it is advanced each time the context's date
(:py:func:`now`) is advanced.

The dirty flags mentioned previously are a bit field. One of these bits is reserverd for
updates to time (:py:func:`now`). When the time changes all nodes that are dependent on
:py:func:`now` have the ``DIRTY_FLAGS.TIME`` bit in their dirty flags set. In addition, any generators or
iterators not dependent on :py:func:`now` also have the ``DIRTY_FLAGS.TIME`` bit in their dirty flags set
(as well as any dependent nodes, as explained in the previous section).

When the evalnode is evaluated and *only* the ``DIRTY_FLAGS.TIME`` bit is set then, if the node is a
generator or iterator, instead of completely re-evaluating the node the previously instantiated
iterator is advanced. The iterator is stored on the :py:class:`NodeState` for the node and context.

Because generators and iterators need to be advanced on *every* timestep, regardless of
whether their value is actually used on any particular timestep, :py:meth:`MDFContext.set_data`
evaluates all of them after marking them as dirty (just with the ``DIRTY_FLAGS.TIME`` bit). Other
nodes could get the value of an iterator node once and then not look at the value for a 
number of timesteps; this would cause problems as the iterator wouldn't have been steped through
the intermediate timesteps and so would have the wrong value, and so evaluating them in the
:py:meth:`MDFContext.set_date` prevents that problem from occurring.

Shifted contexts
================

Shifted contexts are created by shifting any other context (i.e. shifted or non-shifted) via
the :py:meth:`MDFContext.shift` method.

Shift operations are commutative and associative, i.e::

    # commutative
    ctx.shift({a: x}).shift({b: y}) == ctx.shift({b: y}).shift({a: x})

    # associative
    ctx.shift({a: x, b: y}).shift({c: z}) == ctx.shift({a: x}).shift({b: y, c: z})

Shifted contexts are all stored in a flat structure on the **root** context. Shifted contexts
have a parent context, but that parent is *always the root context*. This flat structure
is what facilitates the commutative and associative properties of the shift operation.
There is a method :py:meth:`MDFContext.is_shift_of` to determine if one context is a shift
of another. This is determined by looking at the intersection of the two contexts' shift sets
rather than having an explicit hierachy of contexts.

Each shifted context is keyed by its ``shift_set``, which is the dictionary of nodes to their
values in the shifted context. Any shift resulting in the same net shift set returns the
same shifted context.

Node evaluation in shifted contexts
-----------------------------------

When evaluating nodes in a shifted context the naive approach would be to just evaluate that
node and all its dependencies in that shifted context. This however would cause problems 
where varnodes are set in the root context (or other shifted contexts that the context
the node is being evaluated in is itself a shift of), because the value wouldn't be available in
the shifted context. It would also be inefficient as nodes would potentially be evaluated
multiple times when they are needed in different contexts, even if the value in each case would
be the same because the node doesn't depend on some or all of the shifted nodes.

For these reasons there is the concept of the *alt context*. The alt context is a property
of a node and a context, and is the least shifted context the node can be evaluated in
that will have the same result as if it were evaluted in the orignal context.

For example::

    a = varnode()
    b = varnode()
    
    @evalnode
    def foo():
        return a()


    ctx = MDFContext()
    shifted_a = ctx.shift({a : 1})
    shifted_b = shifted_a.shift({b : 2})

    shifted_b[foo] == shifted_a[foo]

Here evaluating ``foo`` in a context where ``b`` is shifted has no effect, but shifting ``a``
does because ``foo`` depends on ``a``. Therefore the *alt context* for ``shifted_b[foo]``
is ``shifted_a``. 

Even if the context ``shifted_a`` hadn't been explicitly created as above the alt context
would still be that context, i.e. a context with shift set ``{a : 1}``.

Determining the alt context
---------------------------

For varnodes getting the alt context from a shifted context is simple. If the shift set of
the shifted context includes the varnode then the alt context is the parent (root) context
shifted by the varnode and the shift value. All other shifts are irrelevant for the
varnode and so are ignored when getting the alt context.

Evalnodes are a bit tricker as they have to be evaluated once before the dependencies
are known. The first time round they are evaluated in the context they're called in.
Before setting the value of the node in the :py:class:`NodeState` however the 
dependencies are analysed and the least shifted context (alt context) is determined
by checking the dependencies of all the called nodes and the shift set of the 
original context. All the dependencies and state of the node in the original context
is transfered to the newly discovered alt context.

Once the alt context for a node and context has been determined it's cached in the
:py:class:`NodeState`. If a dependency changes then that cached value is cleared
and it will be re-determined the next time it's needed. If a node has conditional
dependencies (i.e. new dependencies are discovered after the initial evaluation)
that can cause the alt context to change, and this causes an an exception to be raised. 
Allowing the alt context to change part way through an evaluation would be danergous as
there could be accumulated state in the original alt context that wouldn't be consistent
in the new alt context.

**********
Node Types
**********

Other node types (e.g. :py:func:`queuenode`, :py:func:`ffillnode`) are built on top of
:py:class:`MDFEvalNode`. The basic concept is that a ``nodetype`` does the job of a normal
evalnode but then calls a second function on the result of that to transform it in some way.

The second function that does that transformation is referred to as the ``node type function``
and the inner function that gets evaluated to provide the input to the ``node type function``
is the ``node function``.

Here's an exampple of a very simple custom node type that will help illustrate how node
types work::

    @nodetype
    def node_type_function(value):
        return value * 2

    @node_type_function
    def node_function():
        return x

When evaluating the node ``node_function`` the order of execution is to first call the
python function ``node_function`` and then to pass the result of that to the python
function ``node_type_function``. The result of ``node_type_function`` becomes the value
of the node ``node_function``.

To understand how this works it helps to talk about what types are used and what
the result of these decorators is.

``nodetype`` returns an instance of a :py:class:`MDFCustomNodeDecorator`. This instance
keeps a reference to the decorated function. It's a callable object and its call
method works as a decorator.

So, the decorated ``node_type_function`` is an instance of :py:class:`MDFCustomNodeDecorator`.
When used as a decorator as ``@node_type_function`` on another function it
returns an instance of :py:class:`MDFCustomNode`. This :py:class:`MDFCustomNode` is a
subclass of :py:class:`MDFEvalNode` as is instantiated with the node function and keeps
a reference to the ``node type function``.
:py:class:`MDFCustomNode` differs from :py:class:`MDFEvalNode` by calling its
node type function after doing the normal evaluation of the node function. The result
of this is what gets returned as the final value of the node.

Things get a little more complicated when adding arguments to the node type,
for example::

    @nodetype
    def node_type_function_2(value, multipler):
        return value * multiplier

    @node_type_function_2(multiplier=2)
    def node_function_2():
        return x

To pass the arguments from the node instantiation (when @node_type_function_2 is
called with node_function_2) to the node type function ``node_type_function_2``
the args have to be stored by the :py:class:`MDFCustomNode` instance. This
is exactly what happens, and when the custom node is evaluated it calls the
node type function with these stored arguments.

If any of the arguments are nodes they automatically get evaluated before
being passed to the node type function. Occasionally it is necessary to
pass nodes in as arguments. :py:class:`MDFCustomNode` checks a class property
``node_kwargs`` and doesn't automatically evaluate any args in that list.
By subclassing :py:class:`MDFCustomNode` this can be set for specific node types.

Generators and iterators
========================

.. sidebar:: MDFIterator

    :py:class:`MDFIterator` is a base class that is recognized by the MDF toolkit as
    being an iterator and is treated in exactly the same way as a generator. The reason
    for using an :py:class:`MDFIterator` instead of a generator is that
    :py:class:`MDFIterator` instances may be pickleable whereas generators are not.

Node type functions may also be a generator or iterator (:py:class:`MDFIterator`).
If they are then the first time the node type function is called it will be
called with the node function results and all the arguments from when the node of
that type was created (e.g. ``muliplier`` in the example from the previous section).

``next()`` is then called to get the initial value. Subsequent values are obtained
by advancing the iterator sending in the new node function result by the ``send()``
method of the generator or :py:class:`MDFIterator` instance.

For this to work correctly the node function that is used to initialize the
base class :py:class:`MDFEvalNode` depends on whether the node type function
or the node function is a generator or not. If either of them are then
the function called by the underlying MDFEvalNode code must return an iterator
that will work in the same way as if the final node was a generator. This is
done using another class, :py:class:`MDFIteratorFactory`. This is another callable
that when called returns a :py:class:`MDFIterator`, which the underlying
:py:class:`MDFEvalNode` code understands and treats like a generator.

Method syntax for node types
============================

When a node type is registered using the :py:func:`nodetype` decorator a method
name can be specified. This automatically adds two new methods to :py:class:`MDFNode`
(and any existing instances) - one that returns a node of the node type and one that
returns the value of a node of that node type.

Both methods work in exactly the same way. A new :py:class:`MDFCustomNode` instance
is constructed with the node type function and using the node the method is called
on as the node function. If the same method is called again with the same arguments
on the same node then it returns the node constructed previously.

It works by creating the new methods when the :py:func:`nodetype` decorator is called.
The methods are actually instances of :py:class:`MDFCustomNodeMethod` which is yet
another callable class. Calling that creates the new node or fetches it if it was
created already.

The new instances of :py:class:`MDFCustomNodeMethod` are added to
:py:attr:`MDFNode._additional_attrs_`, which is checked in :py:attr:`MDFNode.__getattr__`
allowing for new attributes to be added dynamically to the cythoned class. 

***********
Class nodes
***********

Nodes can be declared as properties of classes as well as modules. These nodes may take
a single argument, which will be the class the node is being called on. If they take
no arguments they behave the same way as a normal node.

Class nodes have to be aware of the class they're defined on and the class they're
bound to (accessed from). Consider the following::

    class A(object):
    
        @evalnode
        def foo(cls):
            return "Declared in A, called on %s" % cls.__name__

    class B(A):

        @evalnode
        def foo(cls):
            return "Overridden in B (%s)" % super(B, cls).foo()

In one sense this code just declares two nodes, ``A.foo`` and ``B.foo``. That's
a slight simplication of what's actually going on though; if there were only two then
super(B, cls).foo() would have to evaluate ``A.foo()`` which would return
``Declared in A, called on A``. So, there are actually threee [#]_ nodes, ``A:A.foo``,
``B:B.foo()`` and ``B:A.foo()``.

What the code above declares are `unbound` nodes. That is, nodes that have no
knowledge of the classes they belong to. When they are `accessed` from the class
(i.e. ``A.foo`` accesses foo from A) they are then bound to the class at that
point. Binding creates a new node that includes everything from the original
unbound node definition and information about the class the new node is bound
to.

:py:class:`MDFEvalNode` is a descriptor and so the process of binding nodes to a class
is done by :py:meth:`MDFEvalNode.__get__`. This is called whenever a node is accessed
as a property of a class. All evalnodes have a dictionary of classes to bound versions
of themselves. When accessing a node multiple times on the same class the same bound
node is returned each time. If no bound node exists for the unbound node and class then
a new node is created.

To bind the node to a class the function that it references must also be bound
(to create a staticmethod or classmethod) and the new node is created using that
bound method. As keyword arguments to the node may also reference unbound functions
or nodes those too need to be bound. :py:meth:`MDFEvalNode._bind` handles binding
any additional functions or nodes and may be overridden by any subclasses requiring
additional objects to also be bound. The helper method
:py:class:`MDFEvalNode._bind_function` is used to create bound versions of individual
functions, methods and other callable object types.

.. [#] More accurately there are three `bound` nodes and two `unbound` nodes,
       but the unbound nodes aren't accessible outside of the class definition.
