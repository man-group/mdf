.. py:currentmodule:: mdf

Nodes
=====

The nodes that form the DAG are declared as normal python functions, decorated
with one of the node decorators such as :py:func:`evalnode`.

Nodes are callable objects that take no arguments. Calling them either invokes
the node function or returns the previous cached result if no dependencies of
the node have changed.

Nodes may only be called from other node functions. Calling a node outside of a
node function will result in an error. Evaluating a node outside of another
node function must be done via a context object.

Dependencies between nodes are discovered at run-time as the nodes are
evaluated. The context keeps track of what node is currently being evaluated
and as that node references other nodes it adds the edges to the DAG. If a node
is conditionally evaluated from another node function, that dependency is only
discovered once that condition is met and the branch evaluating the other node
is executed.

Nodes are evaluated from other within other node functions (or any function
called by a node function) by calling them::

    from mdf import evalnode
    
    @evalnode
    def node_function():
        """
        this function is actually a node because of the
        use of the @evalnode decorator
        """
        # to evaluate other nodes they just need to be called
        value = another_node()

        # do some calculation
        result = ...
        return result

    @evalnode
    def another_node():
        # do some calculation possibly involving other nodes
        return result

Time Dependent Nodes
--------------------

Nodes are marked as requiring re-calculation whenever any of their dependencies
are modified. They are later lazily evaluated as required.

There's a builtin node :py:func:`now` that behaves in a more specialized way and
allows node valuations to evolve over time.

When the :py:func:`now` node is advanced, which can be done manually via
:py:func:`MDFContext.set_date`, all the nodes dependent on time are marked as
requiring re-calculation but additionally they are marked that the reason they
require re-calculation is because time has moved forwards.

:py:func:`evalnode` nodes can be generators instead of regular functions
(generators *yield* values rather than *return* a single value). When a
generator is used :py:mod:`mdf` will advance the generator of the node each
time the :py:func:`now` node is advanced. This allows state to be maintained
between valuations::

    from mdf import MDFContext, evalnode, now
    from datetime import datetime, timedelta

    @evalnode
    def time_dependent_node():
        """
        a simple node whose value is dependent on 'now'
        """
        # returns 0, 1, 2, ... for the current weekday
        return now().weekday()

    @evalnode
    def incrementally_updated_node():
        """
        the value of this node is the sum of another node
        """
        # the first value will simply be time_dependent_node
        todays_value = time_dependent_node()
        yield todays_value

        # when the date is advanced this generator is continued
        # until the next yield

        while True:
            # yield today's value + the value evaluated previously
            prev_value = todays_value
            todays_value = time_dependent_node()
            yield todays_value + prev_value

    # create the context with an initial date
    date = datetime(2011, 9, 2)
    ctx = MDFContext(date)

    # get the value of incrementally_updated_node
    x = ctx[incrementally_updated_node]
    # x is now 4 (Friday)

    # advance the date one day
    date += timedelta(days=1)

    # set the date on the context to be invoked (this causes the
    # incrementally_updated_node generator to be advanced)
    ctx.set_date(date)

    # get the value of incrementally_updated_node
    x = ctx[incrementally_updated_node]
    # x is now 9 : 4 (Friday) + 5 (Saturday) = 9

This is a simple example, but the same methods can be used to build more complex
nodes that perform incrementally calculated time-dependent nodes.

If time is ever moved backwards by calling :py:func:`MDFContext.set_date` then
the current state of the time dependent nodes is discarded and the initial
state will be re-evaluated by restarting the generators.

Filtering
~~~~~~~~~

For nodes that update incrementally with time sometimes it's useful to be able
to specify whether the update should be called or not for a particular date
rather than have to check inside the update function.

For example, some values might only need updating on valid business days but the
context might be stepped through all calendar dates for a date range::

    from mdf import evalnode

    def my_node_filter():
        # the filtered evalnode will only be advanced on business days
        if my_is_valid_business_day_function():
            return True
        return False

    @evalnode(filter=my_node_filter)
    def my_node():
        yield some_initial_value
        while True:
            do_some_update_calculation(...)
            yield updated_value

The filter could be a node instead of a function. This is convenient if you need
to apply the same filter to multiple nodes as it won't be re-calculated more
than necessary.

To make it easy to get a filter relating to a specific series of data there's
a function :py:func:`filternode` to create a node that returns ``True`` when
the current date is in the index of that data, or ``False`` otherwise. This
makes it simpler to perform calculations at the frequency of the underlying
data. 

Queue Nodes
-----------

Queue nodes are a specialized time-dependent node. The value of the node is a
double ended queue (see ``collections.deque``) of values. A double ended queue
is used as it supports efficient appending and popping to both sides of the
queue. Queues can also be used to construct numpy arrays and regular python
lists.

The node function is called each time the node :py:func:`now` is advanced and
the result is appended to the queue. The value of the node is the queue itself,
which should be regarded as immutable.

Below is an example that uses a queue to get a delayed value::

    from mdf import evalnode, queuenode

    @queuenode
    def some_value_queue():
        # do some calcuations
        return result

    @evalnode
    def delayed_value():
        values = some_value_queue() # type is collections.deque
        if len(values) < 5:
            return np.nan

        # return the value calculated 4 timesteps ago
        # (the item at -1 is the value for now)
        return values[-5]

Queue nodes can be bounded so they don't grow indefinetely. This is done by
setting the size of the queue. Once the queue reaches that size older items
will be popped off the queue. The size can be specified as either an integer
value or as a callable object (e.g. function or node) which can be useful if
the size if a function of another node. Once the queue is created the size is
fixed for that context::

    # keep at most 5 values
    @queuenode(size=5)
    def some_value_queue():
        # do some calcuations
        return result

    #
    # or calculate the size as a function (or node)
    #

    def get_queue_size():
        return 5

    @queuenode(size=get_queue_size)
    def some_other_value_queue():
        # do some calcuations
        return result

Because queue nodes are a specialization of the eval node, they may also be
filtered in the same way. If a filter is applied only when the filter returns
True will values be calculated and appended to the queue.

Other Node Types
----------------

While eval nodes can be used to calculate any type of value, commonly used
valuation types can be packaged as other node types for convenience. Currently
the list of these specialized nodes is quite small, but as more use-cases are
presented it's reasonable to expect this list to grow.

Because these nodes are all specializations of the eval node, they may also be
filtered in the same way. If a filter is applied only when the filter returns
True will values be calculated or updated.

Delay Node
~~~~~~~~~~

The delay node type is closely related to the queue node type. The
:py:func:`delaynode` node type delays the value returned for a number of
timesteps that can be specified as the ``periods`` parameter to that function::

    from mdf import evalnode, delaynode

    @delaynode(periods=10)
    def a_delayed_value():
        return some_value

    @evalnode
    def some_other_value():
        x = a_delayed_value() # this is the valued returned by a_delayed_value as it
                              # was 10 timesteps ago

The value of the node before the number of periods has elapsed can be set using
the ``initial_value`` parameter. The node's value will be this until enough
timesteps have elapsed. By default the initial value is ``None``.

The function decorated with :py:func:`delaynode` may be called when the node is
evaluated if it hasn't already been called for the current timestep or if any
of its dependencies have changed. This can be a problem if attempting to set
up a recursive relationship such as::

    @delaynode(periods=1, initial_value=0)
    def delayed_a():
        return a()
        
    @evalnode
    def a():
        return 1 + delayed_a()

Even though the value for :py:func:`delayed_a` should be available before
:py:func:`a` is evaluated this still results in an infinite recursion
as evaluating :py:func:`delayed_a` will result in a recursive call to
:py:func:`a`.

To solve this problem delaynodes may optionally be lazily evaluated by
setting the ``lazy`` kwarg to True::

    @delaynode(periods=1, initial_value=0, lazy=True)
    def delayed_a():
        return a()

This is not the default because dependencies are discovered at run-time
and so delaying evaluation of a node will result in dependencies being added
in a later timestep that alter the structure of the DAG. When using
shifted contexts this can be a problem. If ``mdf`` thinks that a node
can use a parent context of a shifted context, and then later the
dependencies change that break that assumption a
``ConditionalDependencyError`` will be thrown.

The way to fix a problem with conditional dependencies is to make them
unconditional. In the case of delayed nodes this can be done by making
the ``initial_value`` an :py:func:`eval_node` that has the same
dependencies (or at least the ones that are sensitive
to the shift) as the delayed node function.

NaN Sum Node
~~~~~~~~~~~~

The :py:func:`nansumnode` node type calculates the sum of the values returned by
its function as :py:func:`now` is advanced. Values that are NaN are excluded
from the sum::

    from mdf import evalnode, nansumnode

    @nansumnode
    def some_value():
        return some_value

    @evalnode
    def sum_of_some_value():
        value_sum = some_value() # this is the sum of 'some_value' for all time steps so far

Cumulative Product Node
~~~~~~~~~~~~~~~~~~~~~~~

The :py:func:`cumprodnode` node type calculates the cumulative product of the
values returned by its function as :py:func:`now` is advanced::

    from mdf import evalnode, cumprodnode

    @cumprodnode
    def some_value():
        return some_value

    @evalnode
    def sum_of_some_value():
        value_prod = some_value()  # this is the cumulative product of 'some_value'
        						   # for all time steps so far

Apply Node
~~~~~~~~~~

The :py:func:`applynode` node type applies an arbitrary function to the value
returned by the node function. You can optionally supply additional args and kwargs
that will be passed in to the function; if any of these arguments are nodes then
they will be evaluated and the result will be passed in.

For example, to add the values of existing nodes A and B::

    A_plus_B = A.apply(operator.add, args=(B,))
    
Or you can get the node::    

    A_plus_B_node = A.applynode(operator.add, args=(B,))

And then chain apply additional nodes to it, such as a cumulative product::
   
    smoothed_A_plus_B = A_plus_B_node.cumprod(...)


*NB:* Unlike most other node types the applynode shouldn't be used as a decorator,
but instead should only be used via the method syntax for node types \
(see :ref:`nodetype_method_syntax`, below).

Method Syntax For Node Types
----------------------------

Creating a new node for simple operations on an existing node can make code look
bloated and difficult to follow. 

For this reason every node type is also exposed as methods on all other nodes.
This is a syntactic helper and the end result is exactly the same as if
a new node using the node type decorator was used.

This is best illustrated by example::

    from mdf import evalnode, cumprod
    from random import random

    @evalnode
    def random_value():
        while True:
            yield random() 

If we wanted to compute the cumulative product of this random value you could do it by
creating a new node using the :py:func:`cumprod` decorator::

    @cumprodnode(half_life=10)
    def cumulative_product_of_random_value():
        return random_value()

But if there are many nodes this can become a bit awkward. Using the method
syntax the same thing can be achieved as follows::

    @evalnode
    def some_other_node():
        ewam_of_random_value = random_value.cumprod(half_life=10)

        # do some more calculation
        return result

When the `cumprod` method on the `random_value` node is called an internal node
is created for that cumulative product calculation. Each subsequent time it's called
that internal node is re-used and so the effect is exactly the same as if
the cumprod node was created explicitly.

All of the standard node types have corresponding methods, and custom node
types can optionally expose themselves as methods.

In addition, there is also a method that returns the internal implicitly
created node. This allows for chaining, e.g.::

    @evalnode
    def some_other_node():
        ewam_of_random_value_node = random_value.cumprodnode(half_life=10)
        delayed_cumprod = ewam_of_random_value_node.delay(periods=10, initial_value=0)

        # do some more calculation
        return result

Or more simply::

    @evalnode
    def some_other_node():
        delayed_cumprod = random_value.cumprodnode(half_life=10).delay(periods=10, initial_value=0)

        # do some more calculation
        return result
