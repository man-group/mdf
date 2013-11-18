"""
Simple double ended queue faster than deque using cython and lists
"""

cdef class cqueue(object):
    cdef list _queue
    cdef int _start
    cdef int _end
    cdef int _size

    # these can be called from plain python, but for performance use
    # the inline functions declared below
    cpdef push(self, x)
    cpdef pop(self)
    cpdef popleft(self)
    cpdef clear(self)

    cpdef sort(self, key=?, reverse=?)

cdef inline cqueue_push(cqueue self, x):
    """push an item on the queue"""
    # if the queue is full get rid of any used elements and expand the queue
    if self._end >= self._size:
        self._queue += [None] * max(1, self._size*2)
        self._size = len(self._queue)

    # push the new item on the right of the queue                        
    self._queue[self._end] = x
    self._end += 1

cdef inline int cqueue_len(cqueue self):
    """return len(queue)"""
    return self._end - self._start

cdef inline cqueue_pop(cqueue self):
    """pop an item off the queue"""
    self._end -= 1
    return self._queue[self._end]

cdef inline cqueue_popleft(cqueue self):
    """pops an item off the left of the queue"""
    value = self._queue[self._start]
    self._start += 1
    return value

cdef inline cqueue_clear(cqueue self):
    """
    resets internal indexes so the queue appears empty
    The internal queue may still contain references, however.
    Use cqueue.clear to clear and remove references.
    """
    self._start = 0
    self._end = 0

cdef inline cqueue_sort(cqueue self, key, reverse):
    return self.sort(key, reverse)
