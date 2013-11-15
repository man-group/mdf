"""
"""

try:
    import cython

    if not cython.compiled:
        def declare(type=None, value=None, **kwds):
            if type is not None and hasattr(type, '__call__'):
                # Do not change to 'if value' since 0 could be a valid argument to type(...) but will evaluate
                # as false.
                if value is not None:
                    return type(value)
                else:
                    return type
            else:
                return value

        cython.declare = declare

except ImportError:
    pass

class DIRTY_FLAGS:
    """
    When a node's dependencies are updated they are marked
    as dirty (needing to be updated).
    """
    NONE = 0x0
    ALL  = ~NONE
    TIME = 0x1
    ERR = 0x2

    @classmethod
    def to_string(cls, mask):
        if mask == cls.NONE:
            return "NONE"
        if mask == cls.ALL:
            return "ALL"

        result = []
        for x in dir(cls):
            if x.upper() == x and x not in ("NONE", "ALL"):
                flag = getattr(cls, x)
                if flag & mask:
                    result.append(x)
        return " | ".join(result)

