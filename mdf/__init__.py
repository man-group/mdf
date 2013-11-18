"""
MDF Data Flow Programming Toolkit

Any changes to this code should be carefully profiled as well
as tested as many of the functions/methods in this code
get called very frequently and so seemingly small changes
can have a significant impact on overall performance.

"""
import sys

__all__ = [
    "MDFContext",
    "get_nodes",
    "shift",
    "make_shift_set",
    "_get_current_context",
    "varnode",
    "vargroup",
    "evalnode",
    "nodetype",
    "queuenode",
    "delaynode",
    "nansumnode",
    "cumprodnode",
    "ffillnode",
    "returnsnode",
    "rowiternode",
    "datanode",
    "filternode",
    "applynode",
    "now",
    "enable_trace",
    "run",
    "to_csv",
    "build_dataframe",
    "get_final_values",
    "plot",
    "scenario",
    "plot_surface",
    "heatmap",
    "CSVWriter",
    "DataFrameBuilder",
    "NodeLogger",

    # settings
    "enable_profiling",
    "allow_duplicate_nodes",
    "disable_custom_pyro_serialization",

    # deprecated functions
    "_shift"
]

from .context import (
    MDFContext,
    shift,
    get_nodes,
    enable_profiling,
    allow_duplicate_nodes,
    make_shift_set,
    _get_current_context,
    _shift,
)

from .nodes import (
    varnode,
    vargroup,
    evalnode,
    now,
    enable_trace
)

from .nodetypes import (
    nodetype,
    queuenode,
    delaynode,
    nansumnode,
    cumprodnode,
    ffillnode,
    returnsnode,
    rowiternode,
    datanode,
    filternode,
    applynode,
    lookaheadnode,
)

from .runner import (
    run,
    to_csv,
    plot,
    scenario,
    plot_surface,
    heatmap,
    build_dataframe,
    get_final_values
)

from .builders import (
    CSVWriter,
    DataFrameBuilder,
    FinalValueCollector,
    NodeLogger,
)

from .remote.serializer import (
    disable_custom_pyro_serialization
)

from . import context
context._lazy_imports()

from . import nodes
nodes._lazy_imports()

#
# register a custom getter for Sphinx Autodoc
# to enable evalnodes to get autodoc'd correctly.
#
if "sphinx" in sys.modules:
    from sphinx.ext.autodoc import AutoDirective
    from sphinx.util.inspect import safe_getattr
    from nodes import MDFEvalNode

    def _evalnode_autodoc_getattr(node, attr, *defargs):
        if attr == "__doc__":
            return node.func_doc
        safe_getattr(node, attr, *defargs)

    AutoDirective._special_attrgetters[MDFEvalNode] = _evalnode_autodoc_getattr
