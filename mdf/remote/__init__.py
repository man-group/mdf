"""
Remote proxy objects for MDF.

Allows contexts to be constructed and evaluated in external processes
using Pyro4. 
"""
from ..context import MDFContext
from ..runner import run
from . import serializer
import Pyro4
import copy
import sys

import Pyro4.util

import pickle
Pyro4.util.Serializer.pickle = pickle

# install the pyro excepthook handler so that we get full remote tracebacks
sys.excepthook=Pyro4.util.excepthook

import logging
_log = logging.getLogger(__name__)

class Proxy(object):
    """
    Basic proxy class that simply forwards all attributes and
    methods to the underlying proxied object.
    """

    def __init__(self, subject):
        self.__dict__["__subject__"] = subject

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        return getattr(self.__dict__["__subject__"], attr)

    def __setattr__(self, attr, value):
        if "__subject__" in self.__dict__ and hasattr(self.__dict__["__subject__"], attr):
            setattr(self.__dict__["__subject__"], attr, value)
            return
        self.__dict__[attr] = value

    def __getstate__(self):
        # if for some reason a proxy object wasn't registered with the Pyro daemon
        # Pyro would try to pickle it to send it to the client, but the whole point
        # of the proxy is that it only exists on the server so make sure that
        # doesn't happen.
        raise Exception("Proxy objects aren't pickleable")

class ContextProxy(Proxy):
    """
    Proxy for MDFContext that replaces some methods so
    it can be used as a Pyro proxy object
    """

    def __init__(self, ctx, remote_api=None):
        Proxy.__init__(self, ctx)
        self.__remote_api = remote_api 
        self.__ctx = ctx
        if remote_api and remote_api.pyro_daemon:
            remote_api.pyro_daemon.register(self)

    def _get_real_context(self, cache=False):
        """
        returns the underlying MDFContext.
        """
        return self.__ctx

    def shift(self, shift_set):
        """overriden to shift the remote context and return a proxy"""
        shifted_ctx = self.__ctx.shift(shift_set)
        return ContextProxy(shifted_ctx, self.__remote_api)

    def get_remote_api(self):
        """returns the remote api object used to get this object"""
        return self.__remote_api

    def run(self, date_range, callbacks=[], values={}, shifts=None, filter=None):
        """see MDFRemoteAPI.run"""
        return self.__remote_api.run(date_range=date_range,
                                     callbacks=callbacks,
                                     values=values,
                                     shifts=shifts,
                                     filter=filter,
                                     ctx=self.__ctx)

    def shutdown(self):
        """shuts down the parent daemon process"""
        self.__remote_api.shutdown()

class SerializedContext(object):
    """
    wraps up serializing a context so it can be done once on the client
    side and then sent to multiple subprocesses
    """

    def __init__(self, ctx):
        self.__serializer = Pyro4.util.Serializer()
        self.__data, self.__compressed = self.__serializer.serialize(ctx)

    def _get_real_context(self):
        return self.__serializer.deserialize(self.__data, self.__compressed)

class MDFRemoteAPI(object):
    """
    Pyro remote object for creating and interacting with mdf
    objects on a remote server.
    """
    def __init__(self, pyro_daemon):
        self.pyro_daemon = pyro_daemon

    def create_context(self, now=None):
        """creates a new remote context and returns a proxy to it"""
        ctx = MDFContext(now)
        return ContextProxy(ctx, self)

    def get_remote_context(self, ctx):
        """
        Takes a local ctx and returns a remote proxy.
        The local context is pickled and unpickled on the remote side.
        """
        return ContextProxy(ctx, self)

    def release_context(self, ctx):
        """
        Unregisters a remote context.
        Call when the context is no longer required.
        """
        if isinstance(ctx, ContextProxy):
            self.pyro_daemon.unregister(ctx)

    def run(self, date_range, callbacks=[], values={}, shifts=None, filter=None, ctx=None, tzinfo=None):
        """
        Remote version of mdf.run.
        - Callbacks must be pickleable remote objects
        - ctx may be a remote context
        - returns (remote ctxs, callbacks)
        """
        # get the real context if passed a proxy
        if not isinstance(ctx, MDFContext):
            # use the cache option so all the other 
            ctx = ctx._get_real_context()
        assert isinstance(ctx, MDFContext)

        # call mdf.run and return proxies to the callbacks
        shifted_contexts = run(date_range,
                                callbacks=callbacks,
                                shifts=shifts,
                                values=values,
                                filter=filter,
                                ctx=ctx,
                                tzinfo=tzinfo)

        # finalize the builders if possible
        for callback in callbacks:
            finalize = getattr(callback, "finalize", None)
            if finalize and callable(finalize):
                finalize()

        # mdf.run returns the original context if there are no shifts, but this
        # method differs slightly and always returns a list of contexts
        if not shifts:
            shifted_contexts = [shifted_contexts]

        shifted_contexts = map(self.get_remote_context, shifted_contexts)
        return shifted_contexts, callbacks

    def shutdown(self):
        """shuts down the parent daemon process"""
        self.pyro_daemon.shutdown()

_daemon = None
def get_daemon():
    """returns the Pyro daemon for the current process"""
    global _daemon
    if _daemon is None:
        _daemon = Pyro4.Daemon()
    return _daemon

def start_server(name=None, pipe=None):
    """
    starts a new Pyro server.
    
    If name is not None it will attempt to locate
    the Pyro nameserver and register this server
    with that name.

    If pipe is not None it should be a multiprocessing
    PipeConnection object and the URI of the remote API
    will be sent to it.
    """
    daemon = get_daemon()
    api = MDFRemoteAPI(daemon)
    uri = daemon.register(api)

    if name:
        ns = Pyro4.locateNS()
        ns.register(name, uri)

    print ("mdf.remote server started")
    print ("URI=%s" % uri)
    if name:
        print ("Name=%s" % name)
    sys.stdout.flush()

    if pipe:
        pipe.send(uri)
        pipe.close()

    daemon.requestLoop()
