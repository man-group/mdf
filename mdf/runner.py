"""
Convenience functions for creating a context and evaluating nodes
for a range of dates and collecting the results.
"""
from .context import MDFContext, NodeOrBuilderTimer, _profiling_is_enabled
from .nodes import MDFNode
from datetime import datetime
import numpy as np
import pandas as pa
import logging
import inspect
import sys
import atexit
import time
import multiprocessing.util
from multiprocessing import Process, Pipe

from matplotlib import cm
import matplotlib.pyplot as pp

from .builders import CSVWriter, DataFrameBuilder, FinalValueCollector

try:
    import win32gui
except ImportError:
    win32gui = None

_logger = logging.getLogger(__name__)

def _create_context(date, values={}, ctx=None, **kwargs):
    if ctx is None:
        ctx = MDFContext(date)
    ctx.set_date(date)
    for node, value in values.items():
        ctx.set_value(node, value)
    for key, value in kwargs.items():
        ctx.set_value(key, value)
    return ctx

def _localize(dt, tzinfo):
    """
    :return: A localized datetime
    :paramd dt: Naive datetime
    :param tzinfo: A pytz or python time zone object

    Attempts to localize the datetime using pytz if possible. Falls back to the builtin
    timezone handling otherwise.
    """
    try:
        return tzinfo.localize(dt)
    except AttributeError:
        return dt.replace(tzinfo=tzinfo)

def run(date_range,
        callbacks=[],
        values={},
        shifts=None,
        filter=None,
        ctx=None,
        num_processes=0,
        tzinfo=None,
        **kwargs):
    """
    creates a context and iterates through the dates in the
    date range updating the context and calling the callbacks
    for each date.

    If the context needs some initial values set they can be
    passed in the values dict or as kwargs.

    For running the same calculation but with different inputs
    shifts can be set to a list of dictionaries of (node -> value)
    shifts.

    If shifts is not None and num_processes is greater than 0 then that many
    child processes will be spawned and the shifts will be processed in parallel.

    Any time-dependent nodes are reset before starting by setting the context's
    date to datetime.min (after applying time zone information if available).
    """
    unshifted_ctx = _create_context(date_range[0], values, ctx, **kwargs)
    contexts = [unshifted_ctx]
    callbacks_per_ctx = {}
    generators_per_ctx = {}

    profiling_enabled = _profiling_is_enabled()

    # The time to use for resetting time dependent nodes. 
    # Note: strftime() methods requires year >= 1900 
    adj_datetime_min = datetime(1900, 1, 1)

    # Attempt to guess the tzinfo from the date range if one isn't specified explicitly
    if tzinfo is None:
        if isinstance(date_range, pa.DatetimeIndex):
            # pa.DatetimeIndex has a tzinfo attribute
            tzinfo = date_range.tzinfo
        elif isinstance(date_range, (list, tuple)):
            # In a list of dates, look at the first item
            tzinfo = date_range[0].tzinfo

    # ensure that any time-dependent nodes are reset before running through
    # the date range by setting the current date on the context to the default.
    unshifted_ctx.set_date(adj_datetime_min if tzinfo is None else _localize(adj_datetime_min, tzinfo))

    if shifts:
        if num_processes > 0:
            return _run_multiprocess(date_range, callbacks, shifts, filter, num_processes, unshifted_ctx)

        # get each shift set as a sorted list so when the shifts are
        # applied they're always done in the same order
        shift_sets = [sorted(x.items()) for x in shifts]

        contexts = []
        for shift_set in shift_sets:
            # create the shifted context and add it to the list
            shifted_ctx = unshifted_ctx.shift(shift_set)
            contexts.append(shifted_ctx)

    for ctx in contexts:
        callbacks_per_ctx[ctx.get_id()] = list(callbacks)

    for date in date_range:
        unshifted_ctx.set_date(date)

        for ctx in contexts:
            # skip dates where the filter doesn't return True
            if filter is not None:
                if not ctx.get_value(filter):
                    _logger.debug("Skipping %s" % date)
                    continue

            _logger.debug("Processing %s %s" % (date, ctx))
            ctx_id = ctx.get_id()

            # advance the generators
            generators = generators_per_ctx.setdefault(ctx_id, [])
            for callback, generator in generators:
                with ctx._profile_builder(callback):
                    generator.send(date)

            # call the callbacks
            found_generator = False
            callbacks = callbacks_per_ctx[ctx_id]
            for i, callback in enumerate(callbacks):
                with ctx._profile_builder(callback):
                    result = callback(date, ctx)

                # if the result is a generator remove this callback from
                # the list of callbacks and add the generator to be advanced
                # next time
                if inspect.isgenerator(result):
                    generator = result
                    callbacks[i] = None
                    generators.append((callback, generator))
                    found_generator = True

                    # advance to the first yield statement
                    generator.next()

            # if any of the callbacks are actually generators remove them
            if found_generator:
                callbacks_per_ctx[ctx_id] = [x for x in callbacks if x is not None]

    if shifts:
        return contexts
    return unshifted_ctx

def _start_remote_server(argv, pipe):
    """
    function for use with multiprocessing.Process object for creating
    a Pyro server
    """
    from .remote import start_server
    start_server(pipe=pipe)

def _run_multiprocess(date_range, callbacks, shifts, filter, num_processes, unshifted_ctx):
    """
    process each context in a pool of processes - called from run
    """
    from .remote import Pyro4, SerializedContext, get_daemon, messaging
    import select

    for callback in callbacks:
        if not hasattr(callback, "combine_result"):
            raise Exception("All callback objects must have a 'combine_result' method")

    num_shifts = len(shifts)
    num_processes = min(num_processes, num_shifts)

    # batch the shifts into a set per-process
    i = 0
    shifts_per_process = {}
    for shift_set in shifts:
        shifts_per_process.setdefault(i, []).append(shift_set)
        i = (i + 1) % num_processes

    def start_proc_thread_func():
        # start a child process running a pyro server and get the uri
        parent_conn, child_conn = Pipe()
        process = Process(target=_start_remote_server, args=(sys.argv, child_conn))
        process.daemon = True
        process.start()
        timeout = time.clock() + 60
        while process.is_alive() and time.clock() < timeout:
            if parent_conn.poll(1):
                break
        else:
            raise Exception("failed to start sub-process")
        uri = parent_conn.recv()
        server = Pyro4.Proxy(uri)
        server._pyroOneway.add("shutdown")
        return process, server

    # multiprocessing expects sys.executable to take  --multiprocessing-fork option if
    # frozen if True, but for us it's always python.exe even if frozen is set
    sys_frozen = getattr(sys, "frozen", False)
    sys.frozen = False
    try:
        # create the pool of processes and pyro servers
        promises = [Pyro4.Future(start_proc_thread_func)() for _ in range(num_processes)]
        processes = map(lambda promise: promise.value, promises)

    finally:
        sys.frozen = sys_frozen

    try:
        # serialize the context once as it could be quite large
        serialized_context = SerializedContext(unshifted_ctx)

        # start running in each process
        batches = []
        for i, (process, remote_api) in enumerate(processes):
            shift_set = shifts_per_process[i]
            batch = Pyro4.batch(remote_api)
            batch.run(date_range,
                      callbacks=callbacks,
                      shifts=shift_set,
                      filter=filter,
                      ctx=serialized_context)
            batches.append(batch)

        # kick off all the asynchronous runs
        future_results = []
        for batch in batches:
            future_results.append(batch(async=True))

        # poll the daemon for this process in case anything is using it while
        # waiting for the results
        while True:
            for batch in future_results:
                if not batch.ready:
                    break
            else:
                break

            # poll the daemon for this process while we wait for results
            daemon = get_daemon()
            read_sockets, unused, unused = select.select(daemon.sockets, [], [], 0.02)
            if read_sockets:
                daemon.events(read_sockets)

            # poll the remote message loop 
            messaging.poll_messages()

        # everything's done, get the results
        try:
            results = [x.value.next() for x in future_results]
        except:
            _logger.error("".join(Pyro4.util.getPyroTraceback()))
            raise

        # re-order the results into the order as the original shifts
        ordered_results = []
        for i, shift_set in enumerate(shifts):
            proc_index = i % num_processes
            remote_ctxs, remote_cbs = results[proc_index]
            ordered_results.append((remote_ctxs.pop(0), remote_cbs, shift_set))

        # combine the results into the local callback objects
        shifted_ctxs = []
        for remote_ctx, remote_callbacks, shift_set in ordered_results:
            local_ctx = unshifted_ctx.shift(shift_set)
            shifted_ctxs.append(local_ctx)
            with remote_ctx:
                for local_cb, remote_cb in zip(callbacks, remote_callbacks):
                    local_cb.combine_result(remote_cb, remote_ctx, local_ctx)

        return shifted_ctxs

    finally:
        # shutdown the child processes
        for process, remote_api in processes:
            with remote_api:
                remote_api.shutdown()

@atexit.register
def _multprocessing_exit():
    """
    The atexit function registered by multiprocessing.util
    is a bit buggy so this one's used instead
    """
    multiprocessing.util._exiting = True
    multiprocessing.util._run_finalizers(0)
    for p in multiprocessing.util.active_children():
        if p._daemonic:
            p._popen.terminate()

    for p in multiprocessing.util.active_children():
        p.join()

    multiprocessing.util._run_finalizers()

def to_csv(fh, date_range, nodes, columns=None, values={}, filter=None, ctx=None, tzinfo=None, **kwargs):
    """
    evaluates a list of nodes for each date in date_range
    and writes the results to a csv file.
    """
    writer = CSVWriter(fh, nodes, columns)
    return run(date_range, [writer], values=values, filter=filter, ctx=ctx, tzinfo=tzinfo, **kwargs)

def build_dataframe(date_range, nodes, values={}, filter=None, ctx=None, tzinfo=None, **kwargs):
    """
    evaluates a list of nodes for each date in date_range
    and returns a dataframe of results
    """
    builder = DataFrameBuilder(nodes)
    run(date_range, [builder], values=values, filter=filter, ctx=ctx, tzinfo=tzinfo, **kwargs)
    return builder.dataframe

def plot(date_range, nodes, values={}, filter=None, ctx=None, tzinfo=None, show=True, plot_args={}, **kwargs):
    """
    evaluates a list of nodes for each date in date_range
    and plots the results using matplotlib.    
    """
    builder = DataFrameBuilder(nodes)
    run(date_range, [builder], values=values, filter=filter, ctx=ctx, tzinfo=tzinfo, **kwargs)
    builder.plot(show=show, **plot_args)

def get_final_values(date_range, nodes, values={}, filter=None, ctx=None, tzinfo=None, **kwargs):
    """
    evaluates a list of nodes for each date in date_range and
    returns a list of final values in the same order as nodes.
    """
    return_as_list = True
    if isinstance(nodes, MDFNode):
        nodes = [nodes]
        return_as_list = False

    collector = FinalValueCollector(nodes)
    ctx = run(date_range, [collector], values=values, filter=filter, ctx=ctx, tzinfo=tzinfo, **kwargs)
    values = collector.get_values(ctx)

    if return_as_list:
        return values
    return values[0]

def scenario(date_range,
                result_node,
                x_node, x_shifts,
                y_node, y_shifts,
                values={}, filter=None, ctx=None, dtype=float, tzinfo=None, **kwargs):
    """
    evaluates a single result_node for each date in date_range and gets
    its final value for each shift in x_shifts and y_shifts.

    x_shifts and y_shifts are values for x_node and y_node respectively.

    result_node should evaluate to a single float, and the result is a 2d nparray
    """
    collector = FinalValueCollector([result_node])

    # build a list of shifts from the x and y shifts
    shifts = []
    for y_value in y_shifts:
        for x_value in x_shifts:
            shifts.append({
                x_node: x_value,
                y_node: y_value
            })

    # collect the values for result_node for all the shifts
    contexts = run(date_range,
                    [collector],
                    shifts=shifts,
                    values=values,
                    filter=filter,
                    ctx=ctx,
                    tzinfo=tzinfo,
                    **kwargs)

    # build a numpy array of the results
    array = np.ndarray(shape=(len(y_shifts), len(x_shifts)), dtype=dtype)
    ctx_iter = iter(contexts)
    for y in range(len(y_shifts)):
        for x in range(len(x_shifts)):
            ctx = ctx_iter.next()
            array[y][x] = collector.get_values(ctx)[0]

    return array

def plot_surface(date_range,
                 result_node,
                 x_node, x_shifts,
                 y_node, y_shifts,
                 values={}, filter=None, ctx=None, dtype=float, tzinfo=None, **kwargs):
    """
    evaluates a single result_node for each date in date_range and gets
    its final value for each shift in x_shifts and y_shifts.

    x_shifts and y_shifts are values for x_node and y_node respectively.

    result_node should evaluate to a single float.

    The results are plotted as a 3d graph and returned as a 2d numpy array.
    """
    results = scenario(date_range,
                        result_node,
                        x_node, x_shifts,
                        y_node, y_shifts,
                        values=values,
                        filter=filter,
                        ctx=ctx,
                        tzinfo=tzinfo,
                        dtype=dtype,
                        **kwargs)

    try:
        X, Y = np.meshgrid(x_shifts, y_shifts)
    except ValueError:
        X, Y = np.meshgrid(range(len(x_shifts)), range(len(y_shifts)))

    fig = pp.figure()
    ax = fig.add_subplot(111, projection="3d")

    ax.plot_surface(X, Y, results, rstride=1, cstride=1, cmap=cm.jet)
    ax.set_xlabel(x_node.name)
    ax.set_ylabel(y_node.name)
    ax.set_zlabel(result_node.name)
    pp.show()

    return results

def heatmap(date_range,
            result_node,
            x_node, x_shifts,
            y_node, y_shifts,
            values={}, filter=None, ctx=None, dtype=float, tzinfo=None, **kwargs):
    """
    evaluates a single result_node for each date in date_range and gets
    its final value for each shift in x_shifts and y_shifts.

    x_shifts and y_shifts are values for x_node and y_node respectively.

    result_node should evaluate to a single float.

    The results are plotted as a heat map and returned as a 2d numpy array.
    """
    results = scenario(date_range,
                        result_node,
                        x_node, x_shifts,
                        y_node, y_shifts,
                        values=values,
                        filter=filter,
                        ctx=ctx,
                        tzinfo=tzinfo,
                        dtype=dtype,
                        **kwargs)

    try:
        X, Y = map(float, x_shifts), map(float, y_shifts)
    except ValueError:
        X, Y = range(len(x_shifts)), range(len(y_shifts))

    pp.figure()

    pp.xlabel(x_node.name)
    pp.xticks(range(len(X)), X)

    pp.ylabel(y_node.name)
    pp.yticks(range(len(Y)), Y)

    pp.imshow(results, interpolation="bicubic")
    pp.grid(True)
    pp.show()

    return results
