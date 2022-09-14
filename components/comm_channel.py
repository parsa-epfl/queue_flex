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
## Author: Mark Sutherland, (C) 2021
import simpy
from simpy import Store
from collections import deque


class CommChannel(object):
    """A class that represents a finite-latency communication channel, can be used by any"""
    """number of producers/consumers on either end."""

    def __init__(self, env, delay):
        self.env = env
        self.delay = delay
        self.store = Store(env)

    def latency(self, value):
        yield self.env.timeout(self.delay)
        self.store.put(value)

    def put(self, value):
        return self.env.process(self.latency(value))

    def get(self):
        return self.store.get()

    def num_items_enqueued(self):
        return len(self.store.items)


def portable_get_q_depth(o):
    if isinstance(o, CommChannel):
        qd = o.num_items_enqueued()
    else:
        qd = len(o.items)
    return qd


def portable_iterate_queued_items(o):
    if isinstance(o, CommChannel):
        return iter(o.store.items)
    elif isinstance(o, deque):
        return iter(o)
    else:  # A simpy object like Store without commchannel wrapper
        return iter(o.items)
