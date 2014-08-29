"""
Runs a Pyro server for remote evaluation of mdf nodes.
"""
import sys
import logging
import pickle
import marshal
import types

_startup_data = None
if __name__ == "__main__":
    # if --fork was specified process the input data and possibly set the
    # pythonpath before importing any non builtin or standard modules
    if "--fork" in sys.argv:
        _startup_data = pickle.load(sys.stdin)

        pythonpath = _startup_data.get("pythonpath", None)
        if pythonpath is not None:
            sys.path = pythonpath

        for modulename in _startup_data.get("modules", []):
            __import__(modulename)
        init_func_s = _startup_data.get("init_func", None)
        if init_func_s is not None:
            init_func_code = marshal.loads(init_func_s)
            init_func = types.FunctionType(init_func_code, globals(), "_mdf_pyro_server_custom_init_func")
            init_func(_startup_data)

# these imports are deliberately after the --fork code as sys.path could be modified
import mdf.remote
import argparse
import sys

def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--service-name",
                            help="if using a pyro nameserver this should be the service name")
    arg_parser.add_argument("--fork",
                            action="store_true",
                            help="used when forked from another process")
    parsed_args = arg_parser.parse_args(sys.argv[1:])
    name = parsed_args.service_name
    pipe = None

    # if _startup_data is not None then finish processing it
    if _startup_data is not None:
        pipe = _startup_data.get("pipe", pipe)
        name = _startup_data.get("service_name", name)

        log_level = _startup_data.get("log_level", None)
        if log_level is not None:
            logging.getLogger().setLevel(log_level)

    mdf.remote.start_server(name, pipe)

if __name__ == "__main__":
    sys.exit(main())
