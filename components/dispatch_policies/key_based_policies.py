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
from .base_policies import find_shortest_q

# Python base package includes
from random import randint
from collections import deque
from ..requests import AbstractRequest
from ..request_filter import QueuedRequestAnalyzerInterface, RequestFilter
from ..comm_channel import portable_get_q_depth


class DispatchNotificationInterface:
    """
    Interface class used by a load balancer to notify a dispatch policy of a directly
    dispatched request that did not use the \"select()\" function.
    """

    def notify_dispatch(self, qdx: int, req: AbstractRequest) -> int:
        self.queue_length_tracking[qdx].appendleft(req)
        return len(self.queue_length_tracking[qdx])


class KeyDispatchPolicy(DispatchNotificationInterface, QueuedRequestAnalyzerInterface):
    def __init__(self, num_queues, queue_objects, num_buckets):
        self.num_queues = num_queues
        self.queue_objects = queue_objects
        self.queue_length_tracking = [deque() for i in range(num_queues)]
        self.func_executed_event = None
        self.num_buckets = num_buckets

    def func_executed(self, q_id, f_type):
        self.queue_length_tracking[q_id].pop()
        if self.func_executed_event is not None:
            self.func_executed_event.succeed()
            self.func_executed_event = None

    def get_reqs_dispatched_to_q(self, q_num: int) -> int:
        """Overrides QueuedRequestAnalyzerInterface.get_reqs_dispatched_to_q(...)"""
        return len(self.queue_length_tracking[q_num])

    def get_reqs_in_private_q(self, q_num: int) -> int:
        """Overrides QueuedRequestAnalyzerInterface.get_reqs_in_private_q(...)"""
        return portable_get_q_depth(self.queue_objects[q_num])

    def filter_reqs_in_private_q(self, q_num: int, f: RequestFilter):
        """Overrides QueuedRequestAnalyzerInterface.filter_reqs_in_private_q(...)"""
        return []


class CRCWDispatchPolicy(KeyDispatchPolicy):
    def __init__(self, num_queues, queue_objects):
        super().__init__(num_queues, queue_objects, num_buckets=1)

    def select(self, req):
        # CRCW doesn't care about keys, just dispatches to the shortest queue
        shortest_q_idx, shortest_len = find_shortest_q(self.queue_length_tracking)
        self.queue_length_tracking[shortest_q_idx].appendleft(req)
        return shortest_q_idx


class CREWDispatchPolicy(KeyDispatchPolicy):
    def __init__(self, num_queues, queue_objects, num_buckets):
        super().__init__(num_queues, queue_objects, num_buckets)

    def select(self, req):
        if req.getWrite():  # have to go to a single queue
            # Map key to partition/queue, by taking its hash value (contained in the req)
            bucket = hash(req) % self.num_buckets
            final_idx = bucket % self.num_queues
        else:  # Can dispatch to shortest queue
            final_idx, shortest_len = find_shortest_q(self.queue_length_tracking)

        self.queue_length_tracking[final_idx].appendleft(req)
        return final_idx


class EREWDispatchPolicy(KeyDispatchPolicy):
    def __init__(self, num_queues, queue_objects, num_buckets):
        super().__init__(num_queues, queue_objects, num_buckets)

    def select(self, req):
        # Map key to partition/queue, by taking its hash value (contained in the req)
        bucket = hash(req) % self.num_buckets
        final_idx = bucket % self.num_queues
        self.queue_length_tracking[final_idx].appendleft(req)
        # print("EREW returning core number {} for req {}. Hash: {}".format(final_idx,req.getID(),hash(req)))
        return final_idx
