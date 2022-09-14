#!/usr/bin/env python
# MIT License

# Copyright (c) 2022, Parallel Systems Architecture Lab (PARSA)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

#!/usr/bin/env python
## Author: Mark Sutherland, (C) 2020
from simpy import Store

# Python base package includes
from random import randint
from collections import deque

from ..comm_channel import CommChannel, portable_get_q_depth


def helper_get_q_depth(q):
    if isinstance(q, deque):
        return len(q)
    else:
        return portable_get_q_depth(q)


def find_shortest_q(qs, filter_list=[], starting_q=0):
    """Returns the queue object, and its length, of the shortest queue provided.
    Optional args are to use a filter list (exclude certain queues), and start from
    an index that is not 0.
    """
    smallest = 1000000000
    shortest_q = 0
    all_q_range = deque(range(len(qs)))
    all_q_range.rotate(starting_q)
    indexes_to_check = [item for item in all_q_range if item not in filter_list]
    for qdx in indexes_to_check:
        c_len = helper_get_q_depth(qs[qdx])
        if c_len < smallest:
            smallest = c_len
            shortest_q = qdx

    return shortest_q, smallest


class RandomDispatchPolicy(object):
    def __init__(self, num_queues):
        self.num_queues = num_queues

    def select(self, req=None):
        the_q_idx = randint(0, self.num_queues - 1)
        return the_q_idx


class JSQDispatchPolicy(object):
    def __init__(self, qs):
        self.queues = qs

    def select(self, req=None):
        shortest_q_idx, the_q = find_shortest_q(self.queues)
        return shortest_q_idx
