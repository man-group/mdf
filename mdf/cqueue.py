"""
fast queue with inlined cython functions
"""
import cython

class cqueue(object):
    """
    simple queue object.
    
    Python methods are provided, but for speed use the inline
    functions defined in cqueue.pyx.
    """

    def __init__(self, initial_size=100):
        c_initial_size = cython.declare(int, initial_size)
        self._queue = [None] * c_initial_size
        self._start = 0
        self._end = 0
        self._size = c_initial_size

    def __len__(self):
        return cqueue_len(self)

    def __iter__(self):
        return iter(self._queue[self._start:self._end])

    def __nonzero__(self):
        return self._end > self._start

    def __getitem__(self, i):
        i_ = cython.declare(int, i)
        if i_ < 0:
            return self._queue[self._end + i_]
        return self._queue[self._start + i_]

    def push(self, x):
        cqueue_push(self, x)

    def pop(self):
        return cqueue_pop(self)

    def popleft(self):
        return cqueue_popleft(self)

    def clear(self):
        cqueue_clear(self)
        self._queue = [None] * self._size

    def sort(self, key=None, reverse=False):
        self._queue[self._start:self._end] = sorted(self._queue[self._start:self._end],
                                                    key=key,
                                                    reverse=reverse)

"""
#
# Uncomment this code to debug mdf without any compiled extensions.
# (usually it's enough to compile cqueue.pyd and leave the others)
#
from collections import deque

def cqueue(initial_size=100):
    return deque()

cqueue_push = lambda q, x: q.append(x)
cqueue_pop = lambda q: q.pop()
cqueue_popleft = lambda q: q.popleft()
cqueue_len = lambda q: len(q)
cqueue_clear = lambda q: q.clear()

def cqueue_sort(queue, key, reverse):
    sorted_queue = sorted(queue, key=key, reverse=reverse)
    queue.clear()
    queue.extend(sorted_queue) 
"""

