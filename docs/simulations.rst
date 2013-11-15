.. py:currentmodule:: mdf

Back-Testing and Scenario Analysis
==================================

Back-Testing
------------

In the context of mdf, a back test is nothing more than evaluating a node or
collection of nodes for a range of dates. Its possible to do this with a simple
for loop::

    import mdf
    import pandas as pa
    from datetime import datetime

    # non-existent module used just for illustration in this example
    import example_nodes
    
    # get the range of dates used for the back test
    date_range = pa.DateRange(datetime(2000, 1, 1), datetime(2011, 9, 6))
    
    # create the context and set some initial values
    ctx = mdf.MDFContext()
    ctx[example_nodes.an_example_var_node] = 5
    
    # evaluate a node for every date in date_range
    results = []
    for date in date_range:
        ctx.set_date(date)
        value = ctx[example_nodes.an_example_evalnode]
        results.append(value)

    # now we can create a dataframe or plot the values etc.

To simplify this :py:mod:`mdf` provides several higher level functions that
do this for you:

build_dataframe
~~~~~~~~~~~~~~~
::

    # evaluate a node and return a dataframe of the results
    # the column name is the node name
    df = mdf.build_dataframe(date_range, example_nodes.an_example_evalnode)

    # or supply a list of nodes
    df = mdf.build_dataframe(date_range,
    							 [example_nodes.an_example_evalnode,
                                  example_nodes.another_example_evalnode]
                                 ctx=ctx)

See :py:func:`build_dataframe`

plot
~~~~
::

    # plot is the same as build_dataframe but it plots the results
    mdf.plot(date_range,
             example_nodes.an_example_evalnode,
             ctx=ctx)

See :py:func:`plot`

to_csv
~~~~~~
::

    # to_csv is the same as build_dataframe but it writes the results to a csv file
    fh = open("myfile.csv")
    df = mdf.to_csv(fh,
                    date_range,
                    example_nodes.an_example_evalnode,
                    ctx=ctx)

See :py:func:`to_csv`

get_final_values
~~~~~~~~~~~~~~~~
::

    # get_final_values steps through all the dates in the range but only returns
    # the final result
    value = mdf.get_final_values(date_range,
                                     example_nodes.an_example_evalnode,
                                     ctx=ctx)

    values = mdf.get_final_values(date_range,
                                      [example_nodes.an_example_evalnode,
                                       example_nodes.another_example_evalnode],
                                      ctx=ctx)

See :py:func:`get_final_values`

run
~~~

:py:func:`run` is the most general of the back testing functions, and in fact is
used by all the other functions.

Instead of producing a particular output format it simply advances the context's
date through the given date range and calls the callables. The callables are
responsible for evaluating any nodes and collecting the results.

Several callbable object classes are provided for use with :py:func:`run`::


    # DataFrameBuilder can be used for collecting values into a dataframe
    df_builder = mdf.DataFrameBuilder([example_nodes.an_example_evalnode,
                                           example_nodes.another_example_evalnode])

    # CSVWriter can be used for writing values to a csv file
    csv_builder = mdf.CSVWriter("myfile.csv",
                                    [example_nodes.an_example_evalnode,
                                    example_nodes.another_example_evalnode])

    # or you can use custom functions as well
    def my_func(ctx):
        print ctx.get_date(), ctx[example_nodes.an_example_evalnode]

    # they're all run in one go using run
    mdf.run(date_range, [df_builder, csv_builder, my_func], ctx=ctx)
	# you can then get the dataframe
	df = df_builder.get_dataframe(ctx)
	
See the API docs for :py:class:`DataFrameBuilder`, :py:class:`CSVWriter` and
:py:func:`run` for more information.


Scenario Analysis
-----------------

As calculations done using :py:mod:`mdf` are constructed as a DAG all the
inputs are accessible, and this lends itself very convieniently to doing
scenario analysis.

Using :py:mod:`mdf` it's possible to run a back-test for a date range with
multiple sets of input parameters simultaneously. By using shifted contexts to
achieve this anything not depedendent on the input parameters being varied
intermediate calculations can be shared, potentially making the overall
run-time less than if the code was run N times with the different input
paramters.

Rather than creating all the shifted contexts and iterating through the date
range each time you want to run a scenario the :py:func:`run` function may be
used::

    # calculate example_nodes.an_example_evalnode for a range of different
    # values for example_nodes.an_example_var_node
    
    # each scenario is specified as a 'shift' dictionary which is a dictionary
    # of nodes to shifted values. In this case only one node is shifted but it
    # could be multiple.
    
    shifts = [
        {example_nodes.an_example_var_node : 1},
        {example_nodes.an_example_var_node : 2},
        {example_nodes.an_example_var_node : 3},
        {example_nodes.an_example_var_node : 4},
    ]
    
    # create a dataframe builder to collect the results of the scenarios
    df_builder = mdf.DataFrameBuilder(example_nodes.an_example_evalnode)

    # run all the scenarios
    mdf.run(date_range, [df_builder], shifts=shifts, ctx=ctx)

    # df_builder.dataframes is now a list of dataframes, one for each shift set

scenario
~~~~~~~~

For the cases where you want to calculate the final result of a node after
iterating through a range of dates you can use the :py:func:`scenario` function.

:py:func:`scenario` takes two nodes to be varied and two lists of values for
those nodes.

The value of the result node should be a scalar value and the result is returned
as a 2d numpy array::

    # artificial example just for illustration
    a = varnode()
    b = varnode()

    @evalnode
    def X():
        return a() + b()

    # calculate the value of X for a in [1, 2, 3, 4] and b in [10, 20, 30, 40]
    a_values = [1, 2, 3, 4]
    b_values = [10, 20, 30, 40]

    results = mdf.scenario(date_range,
                                X,
                                a, a_values,
                                b, b_values,
                                ctx=ctx)

plot_surface
~~~~~~~~~~~~

:py:func:`plot_surface` works in the same way as :py:func:`scenario` except that
the result is plotted as a 3d graph as well as returned as a numpy array::

    # plots the results of each shift as a 3d surface
    results = mdf.plot_surface(date_range,
                                   X,
                                   a, a_values,
                                   b, b_values,
                                   ctx=ctx)

heatmap
~~~~~~~

:py:func:`heatmap` works in the same way as :py:func:`scenario` except that the
result is plotted as a heatmap as well as returned as a numpy array::

    # plots the results of each shift as a heatmap
    results = mdf.heatmap(date_range,
                              X,
                              a, a_values,
                              b, b_values,
                              ctx=ctx)