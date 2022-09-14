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

from ..requests import FuncRequest
from ..comm_channel import CommChannel
from math import ceil

import collections
import typing


def func_type_portable(f):
    if isinstance(f, FuncRequest):
        return f.getFuncType()
    else:
        return f


def portable_iterator(q):
    if isinstance(q, CommChannel):
        return q.store.items
    elif isinstance(q, collections.deque):
        return q
    else:  # a simpy store
        return q.items


def queue_string(q):
    return "[" + ", ".join([str(x) for x in portable_iterator(q)]) + "]"


def ascii_histogram(seq: collections.Counter, scale: int) -> None:
    """A horizontal frequency-table/histogram plot. Prints a + for each \'scale\' values."""
    for k in sorted(seq):
        print("{} {}".format(k, "+" * ceil(seq[k] / scale)))
