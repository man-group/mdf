"""
Functions for building regression tests for mdf based code.

Regression testing is done by evaluating the same nodes
over a range of dates in two different virtual envs.

The different virtual envs are used by starting python
in subprocesses in the virtual envs and run a pyro server
that serves remote context objects (see mdf.remote).
"""
import sys
import os
import getpass
import logging
import atexit
import marshal
import pkg_resources
import tempfile
import types
import threading
import platform
import time
import select
from subprocess import Popen, PIPE, STDOUT
from ..context import MDFContext
from ..nodes import evalnode
from .differs import *

try:
    import cPickle as pickle
except ImportError:
    import pickle


# Import Pyro from the remote package to ensure that the serializer patching 
# and configuration that we do there gets done
from ..remote import Pyro4, get_daemon

__all__ = [
    "get_contexts",
    "run",
    "Differ",
    "DataFrameDiffer",
]

_logger = logging.getLogger(__name__)

_is_x64 = platform.architecture()[0] == "64bit"

_python_exes = {
    "win32"     : r"C:\VirtualEnvs%s\%%s\Scripts\python.exe" % (
                    ("_x64" if _is_x64 else "")),
    "linux2"    : "%s/virtualEnvs/%%s/bin/python" % (
                    os.environ.get("HOME", "/users/%s" % getpass.getuser()))
}

NoneType = type(None)

_running_processes = []
@atexit.register
def _cleanup_regression_processes():
    while _running_processes:
        process = _running_processes.pop()
        if process.poll() is None:
            process.terminate()

_temp_files = []
@atexit.register
def _cleanup_temp_files():
    while _temp_files:
        path = _temp_files.pop()
        if os.path.exists(path):
            os.unlink(path)

def _fh_redirect(fh_in, fh_out, prefix):
    """
    thread function for redirecting one filehandle to another
    """
    # using a fh as an iterator does some internal buffering,
    # so this isn't equivalent to doing 'for line in fh_in...'
    while not fh_in.closed:
        line = fh_in.readline()
        if not line:
            break
        fh_out.write(prefix + " : " + line.strip("\r\n") + "\n")

def _start_pyro_subprocess(python_exe, side, modulenames=[],
                 init_func=None, startup_data={}):
    """
    starts a mdf.remote pyro server and returns
    a Pyro Proxy object.
    
    If python_exe is None the current interpreter is used.
    
    side is for information and is "LHS" or "RHS"
    """
    # dictionary of settings to be passed to the child process
    _start_data = {
        "log_level" : logging.getLogger().getEffectiveLevel(),
        "modules"   : modulenames,
    }
    if init_func is not None:
        _start_data["init_func"] = marshal.dumps(init_func.func_code)
    _start_data.update(startup_data)


    if python_exe is None:
        # use the current interpreter and environment
        python_exe = sys.executable
        env = os.environ
        _start_data["pythonpath"] = sys.path

        # get the script from the metadata and write it to a tempfile
        dist = pkg_resources.require("mdf")[0]
        try:
            script_contents = dist.get_metadata("scripts/mdf_pyro_server.py")
        except IOError:
            # if the metadata version wasn't found it's probably
            # running from a local checkout so look for the script
            # relative to this file
            path = os.path.dirname(__file__)
            script = os.path.join(path, "..", "..", "bin", "mdf_pyro_server.py")
        else:
            # got the script from the metadata, now write it to a tempfile
            fd, script = tempfile.mkstemp(".py")
            _temp_files.append(script)
    
            fh = os.fdopen(fd, "wt")
            fh.write(script_contents)
            fh.close()
    else:
        script = os.path.join(os.path.dirname(python_exe), "mdf_pyro_server.py")

        # create a clean environment to start python in
        env = dict(os.environ)
        for x in ("PYTHONPATH",
                  "PYTHONHOME",
                  "PYDEV_CONSOLE_ENCODING",
                  "PYDEV_COMPLETER_PYTHONPATH"):
            env.pop(x, None)

        # add the virtualenv specific bits
        env.update({
            "PATH"          : ";".join((os.path.dirname(python_exe), os.environ.get("PATH", ""))),
            "PYVPATH"       : os.path.dirname(python_exe),
            "VIRTUAL_ENV"   : os.path.abspath(os.path.join(os.path.dirname(python_exe), "..")),
        })

    if not os.path.exists(script):
        raise IOError("'%s' not found" % script)

    cmd = [python_exe, "-u", script, "--fork"]

    # start the child process (bufsize=1 is line-buffered)
    child_process = Popen(cmd, env=env, stdin=PIPE, stdout=PIPE, stderr=PIPE, bufsize=1)
    _running_processes.append(child_process)

    stderr_thread = threading.Thread(target=_fh_redirect, args=(child_process.stderr,
                                                                sys.stderr,
                                                                side))
    stderr_thread.daemon = True
    stderr_thread.start()

    # send the data to the new process and get the result from the pipe
    child_process.stdin.write(pickle.dumps(_start_data))
    child_process.stdin.close()

    # read the URI from the child stdout
    uri = None
    while uri is None and child_process.poll() is None:
        line = child_process.stdout.readline()
        _logger.debug(line)
        if line.startswith("URI="):
            unused, uri = line.strip().split("=")
            break

    if child_process.returncode is not None:
        raise RuntimeError("Failed to launch subprocess, exited with code %d: %s" % (
                                child_process.returncode, " ".join(cmd)))

    stdout_thread = threading.Thread(target=_fh_redirect, args=(child_process.stdout,
                                                                sys.stdout,
                                                                side))
    stdout_thread.daemon = True
    stdout_thread.start()

    return Pyro4.Proxy(uri)

def _get_context(virtualenv, ctx, side, executable=None, modulenames=[],
                 init_func=None, startup_data={}):
    # get the executables from the virtualenvs
    if executable is None and virtualenv is not None:
        executable = _python_exes[sys.platform] % virtualenv

    # use subprocess to start new processes using the virtualenv python.exe
    server = _start_pyro_subprocess(executable, side, modulenames,
                                    init_func=init_func, startup_data=startup_data)

    # create the remote context and return
    ctx = server.get_remote_context(ctx)
    ctx._pyroOneway.add("shutdown")
    return ctx

def get_contexts(lhs_virtualenv, rhs_virtualenv,
                 lhs_executable=None, rhs_executable=None,
                 lhs_modulenames=[], rhs_modulenames=[],
                 ctx=None,
                 init_func=None, startup_data={}):
    """
    returns a tuple of remote contexts using different
    virtualenvs or python executables.
    
    These remote contexts reference the code in each of their
    respective virtual environments and so can be used
    to evaluate nodes and check for differences.

    lhs_modulenames and rhs_modulenames will be imported
    by the remote processes.

    If ctx is not None it will be copied to both remote
    instances and used as the base context. For that to work
    it must only contain nodes that are available in both
    environments.

    init_func is a function which will be called while starting mdf_pyro_server
    startup_data - dict of data to be passed to init_func
    Note: startup_data is extended with modules, pythonpath etc before passing to init_func
   """
    # get the executables from the virtualenvs
    if lhs_executable is None and lhs_virtualenv is not None:
        lhs_executable = _python_exes[sys.platform] % lhs_virtualenv

    if rhs_executable is None and rhs_virtualenv is not None:
        rhs_executable = _python_exes[sys.platform] % rhs_virtualenv

    if ctx is None:
        ctx = MDFContext()

    # use subprocess to start two new processes using the virtualenv python.exe
    lhs_promise = Pyro4.Future(_start_pyro_subprocess)(lhs_executable,
                                                        side="LHS",
                                                        modulenames=lhs_modulenames,
                                                        init_func=init_func, startup_data=startup_data)

    rhs_promise = Pyro4.Future(_start_pyro_subprocess)(rhs_executable,
                                                        side="RHS",
                                                        modulenames=rhs_modulenames,
                                                        init_func=init_func, startup_data=startup_data)

    lhs_server = lhs_promise.value
    rhs_server = rhs_promise.value

    # create the remote contexts and return
    lhs_ctx = lhs_server.get_remote_context(ctx)
    rhs_ctx = rhs_server.get_remote_context(ctx)

    lhs_ctx._pyroOneway.add("shutdown")
    rhs_ctx._pyroOneway.add("shutdown")

    return lhs_ctx, rhs_ctx

def run(date_range, differs, lhs, rhs, filter=None, ctx=None,
            lhs_modulenames=[], rhs_modulenames=[],
            init_func=None, startup_data={}):
    """
    evaluates the 'differ' objects in two contexts, lhs and rhs.

    lhs and rhs may be strings in which case they should be the
    names of virtual envs which will be used to create the two
    remote contexts, or they may be remote context objects.

    differs is a list of Differ instances that behave like the
    builder objects used by mdf.run, except they can also
    be used to find the difference between the data they collect.
    
    Returns a tuple of:
        - list of tuples, where each tuple is the result
          of the diff and the lhs and rhs differ objects returned by
          the remote processes.
        - lhs approximate elapsed time
        - rhs approximate elapsed time
    
    lhs_modulenames and rhs_modulenames are list of modules that
    need to be imported on the remote sides.

    init_func is a function which will be called while starting mdf_pyro_server
    startup_data - dict of data to be passed to init_func
    Note: startup_data is extended with modules, pythonpath etc before passing to init_func
    """
    if ctx is None:
        ctx = MDFContext()

    shutdown_lhs = False
    shutdown_rhs = False

    if isinstance(lhs, (basestring, NoneType)) \
    and isinstance(rhs, (basestring, NoneType)):
        # get both remote contexts from the virtual env name
        lhs, rhs = get_contexts(lhs, rhs, ctx=ctx,
                                lhs_modulenames=lhs_modulenames,
                                rhs_modulenames=rhs_modulenames,
                                init_func=init_func, startup_data=startup_data)
        shutdown_lhs = shutdown_rhs = True
    elif isinstance(lhs, (basestring, NoneType)):
        # only need the lhs, rhs must already be a remote ctx
        lhs = _get_context(lhs, ctx, side="LHS", modulenames=lhs_modulenames,
                           init_func=init_func, startup_data=startup_data)
        shutdown_lhs = True
    elif isinstance(rhs, (basestring, NoneType)):
        # only need the rhs, lhs must already be a remote ctx
        rhs = _get_context(rhs, ctx, side="RHS", modulenames=rhs_modulenames,
                           init_func=init_func, startup_data=startup_data)
        shutdown_rhs = True

    lhs_ctx, rhs_ctx = lhs, rhs

    try:
        lhs_batch, rhs_batch = map(Pyro4.batch, (lhs_ctx, rhs_ctx))

        lhs_batch.run(date_range, callbacks=differs, filter=filter)
        rhs_batch.run(date_range, callbacks=differs, filter=filter)

        lhs_start = rhs_start = time.time()
        lhs_future = lhs_batch(async=True)
        rhs_future = rhs_batch(async=True)

        lhs_done = rhs_done = False
        while not (lhs_done and rhs_done):
            if not lhs_done and lhs_future.ready:
                lhs_end = time.time()
                lhs_done = True
                try:
                    unused, lhs_result = lhs_future.value.next()
                except:
                    _logger.error("Unable to compute LHS of regression test")
                    raise

            if not rhs_done and rhs_future.ready:
                rhs_end = time.time()
                rhs_done = True
                try:
                    unused, rhs_result = rhs_future.value.next()
                except:
                    _logger.error("Unable to compute RHS of regression test")
                    raise

            # poll the daemon for this process while we wait for results
            daemon = get_daemon()
            read_sockets, unused, unused = select.select(daemon.sockets, [], [], 0.02)
            if read_sockets:
                daemon.events(read_sockets)

        results = []
        for lhs_differ, rhs_differ in zip(lhs_result, rhs_result):
            diff = lhs_differ.diff(rhs_differ, lhs_ctx, rhs_ctx)
            results.append((diff, lhs_differ, rhs_differ))

        lhs_time = lhs_end - lhs_start
        rhs_time = rhs_end - rhs_start

        return results, lhs_time, rhs_time 

    finally:
        if shutdown_lhs:
            lhs_ctx.shutdown()
        if shutdown_rhs:
            rhs_ctx.shutdown()
