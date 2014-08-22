"""
API for sending messages from child processes back to
the main process.
"""
import os
import logging
import sys
import atexit
from decorator import decorator
from collections import namedtuple
from .serializer import Serializer

try:
    import zmq
except ImportError:
    zmq = None

_log = logging.getLogger(__name__)

# global zmq context
ConnectionDetails = namedtuple("ConnectionDetails", "context sink source")
_conn_details = None
_initialized = False
_serializer = None
_handlers = {}

# do the initialization lazily only if something calls this API
def _init():
    global _conn_details, _initialized, _serializer
    assert not _initialized, "already initialized"

    if not zmq:
        raise RuntimeError("zmq not available")

    _serializer = Serializer() 

    context = zmq.Context()
    sink = None
    source = None

    if "MDF_REMOTE_MESSAGING_ADDR" not in os.environ:
        # this must be the parent process so create the sink
        sink  = context.socket(zmq.PULL)
        port = sink.bind_to_random_port("tcp://*")
        _log.debug("Listening on tcp://localhost:%d" % port)

        # set the port as an environment variable so any child processes know
        # what port to connect to.
        os.environ["MDF_REMOTE_MESSAGING_ADDR"] = "tcp://localhost:%d" % port

    else:
        # this must be a child process so connect to the parent sink
        address = os.environ["MDF_REMOTE_MESSAGING_ADDR"]
        source  = context.socket(zmq.PUSH)
        source.connect(address)

    # create the global connection details
    _conn_details = ConnectionDetails(context=context,
                                      sink=sink,
                                      source=source)
    _initialized = True
    @atexit.register
    def cleanup_sockets():
        _conn_details.sink.close()


@decorator
def needs_zmq(func, *args, **kwargs):
    if not _initialized:
        _init()
    return func(*args, **kwargs)

@needs_zmq
def register_message_handler(subject, callback):
    """
    register a message handler to process messages send via send_message.
    """
    _handlers.setdefault(subject, []).append(callback)

@needs_zmq
def unregister_message_handler(subject, callback):
    """
    unregister a message handler registered with register_message_handler
    """
    handlers = _handlers.get(subject, [])
    if callback not in handlers:
        raise RuntimeError("%s/%s handler not registered" % (subject, callback))
    handlers.remove(callback)

@needs_zmq
def send_message(subject, message):
    """
    Send a message to the parent process to be handled by a registered handler.
    If called from the parent process the message will be handled immediately.
    """
    if not _conn_details.source:
        # we're in the parent process so just call the handler
        for handler in _handlers.get(subject, []):
            handler(subject, message)
        return

    # otherwise send it via zmq
    data, compressed = _serializer.serialize((subject, message))
    _conn_details.source.send(data, flags=zmq.SNDMORE)
    _conn_details.source.send_json(compressed)

@needs_zmq
def poll_messages():
    """
    Call periodically from the parent process to check for messages
    and call and handlers.
    """
    while True:
        # get next waiting message
        try:
            # don't block on data, but once we have the data wait for the compressed flag
            data = _conn_details.sink.recv(flags=zmq.NOBLOCK)
            compressed = _conn_details.sink.recv_json()
        except zmq.ZMQError:
            # still waiting for data?
            e = sys.exc_info()[1]
            if e.errno == zmq.EAGAIN:
                return
            raise

        # deserialize and call the handlers
        subject, message = _serializer.deserialize(data, compressed)
        for handler in _handlers.get(subject, []):
            handler(subject, message)
