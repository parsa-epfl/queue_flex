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
from .JBSQ import JBSQDispatchPolicy, JBSQWithStatistics
from ..requests import FuncRequest
from .util import func_type_portable, queue_string

# Python base package includes
from random import randint
from collections import deque


class FunctionDispatch(object):
    def __init__(self, qs, func_grouping):
        self.queues = qs
        self.func_grouping = func_grouping

    def select(self, req):
        req.setAffinity()
        # Map function idx to q
        q_idx = int(req.getFuncType() / self.func_grouping)
        return q_idx


# This dispatch policy implements 'shortest service time' dispatch, with oracle
# knowledge of the service time generator objects. Queueing not taken into account.
class SSTDispatch(object):
    def __init__(self, qs):
        self.queues = qs

    def set_stime_objects(self, stime_objects):
        self.stime_generators = stime_objects

    def select(self, req):
        shortest_time = 1000000000
        shortest_q = 0
        for qdx in range(len(self.stime_generators)):
            t = self.stime_generators[qdx].peek_stime(req.getFuncType())
            if t < shortest_time:
                shortest_time = t
                shortest_q = qdx

        return shortest_q


class AffinityDispatch(object):
    def __init__(self, qs, history_length, max_len):
        self.queues = qs
        self.histories = [[] for q in range(len(self.queues))]
        self.history_length = history_length
        self.max_affinity_q_len = max_len
        self.disp_decisions = 0
        self.decisions_no_history = 0
        self.decisions_affinity = 0
        self.decisions_with_multiple_affinity = 0
        self.decisions_load_balance = 0

    def qs_with_target_function(self, func_id):
        retlist = []
        for idx in range(len(self.queues)):
            if func_id in self.histories[idx]:
                retlist.append(idx)
        return retlist

    def get_statistics(self):
        return {
            "total decisions": self.disp_decisions,
            "history q misses": self.decisions_no_history,
            "affinity": self.decisions_affinity,
            "mult affinity": self.decisions_with_multiple_affinity,
            "lbalance": self.decisions_load_balance,
        }

    def select(self, req):
        queues_with_history = self.qs_with_target_function(req.getFuncType())
        # print('---- NEW REQ w. TYPE ------',req.getFuncType())
        if len(queues_with_history) == 0:
            final_queue, unfilt_len = find_shortest_q(self.queues)
            self.decisions_no_history += 1
            # print('final choice LBALANCE, NOHIST:',final_queue)
        else:
            shortest_q_index_filtered, filt_len = find_shortest_q(
                self.queues, filter_list=queues_with_history
            )
            shortest_q_index, unfilt_len = find_shortest_q(self.queues)
            # print('Shortest q index w. function filter:',shortest_q_index_filtered,'len',filt_len,'global shortest q:',shortest_q_index,'len',unfilt_len)

            if (
                filt_len >= self.max_affinity_q_len
            ):  # past static q length, pick global shortest
                final_queue = shortest_q_index
                # print('final choice LBALANCE, QLEN EXCEEDED:',final_queue)
                self.decisions_load_balance += 1
            else:
                final_queue = shortest_q_index_filtered  # return the shortest among those with history
                req.setAffinity()
                # print('final choice AFFINITY:',final_queue)
                self.decisions_affinity += 1
                if len(queues_with_history) > 1:
                    self.decisions_with_multiple_affinity += 1

        # push this func type into the appropriate history
        self.histories[final_queue].append(req.getFuncType())
        if len(self.histories[final_queue]) > self.history_length:
            self.histories[final_queue].pop(0)  # remove head

        self.disp_decisions += 1

        return final_queue


## A class which adds affinity dispatch to the baseline JBSQ. It records:
## - total number of dispatch decisions
## - for each dispatch decision, was it possible to choose a queue with 'affinity'
## - a sample of all of the dispatch queue lengths over the duration of the simulation
## This class only works with requests of class FuncRequest, as it uses "getFuncType"
class JoinAffinityQueue(JBSQWithStatistics):
    def __init__(
        self, env, qs, bound, sample_interval=1, thresh=1, lbal_thresh=2, history_len=2
    ):
        super().__init__(env, qs, bound, sample_interval, thresh)

        # Model of input/history queues all comes from superclass JBSQWithStatistics
        # input queues [ F0, F1 ]  -> C -> history queues [ F1, F1 ]
        self.load_balance_threshold = lbal_thresh

        # Extra statistics
        self.decisions_load_balance = 0
        self.affinity_input_queue = 0
        self.affinity_history_queue = 0

    def get_statistics(self):
        rd = super().get_statistics()
        rd["decisions_load_balance"] = self.decisions_load_balance
        rd["affinity_input_q"] = self.affinity_input_queue
        rd["affinity_history_q"] = self.affinity_history_queue
        return rd

    def search_affinity(self, f_type):
        found_affinity = False
        num_found = 0
        unique_funcs = list()
        queues_with_affinity = set()
        affinity_in_input = False
        for i in range(self.num_queues):
            unique_funcs.append(list())

        for i in range(self.num_queues):
            # print('looking for affinity to func',f_type,'in input queue idx',i)
            # print('input queue:',self.input_queues[i])
            success, unique_funcs[i] = self.scan_q(
                self.queue_length_tracking[i], f_type, unique_funcs[i]
            )
            if success is True:
                num_found += 1
                found_affinity = True
                affinity_in_input = True
                queues_with_affinity.add(i)

        for i in range(self.num_queues):
            # print('looking for affinity to func',f_type,'in HISTORY queue idx',i)
            # print('regular queue:',self.history_queues[i])
            success, unique_funcs[i] = self.scan_q(
                self.history_queues[i], f_type, unique_funcs[i]
            )
            if success is True:
                if (
                    i not in queues_with_affinity
                ):  # If found in input queues, don't count in history
                    num_found += 1
                    queues_with_affinity.add(i)
                found_affinity = True

        return found_affinity, affinity_in_input, num_found, queues_with_affinity

    def select(self, req=None):
        # Take statistics if interval expired
        self.sample_interval -= 1
        if self.sample_interval == 0:
            self.long_req_depth.append(self.get_num_long_reqs())
            self.sample_interval = self.reset_val

        # Check affinity possible
        (
            found_affinity,
            affinity_in_input,
            num_possible,
            queues_with_affinity,
        ) = self.search_affinity(req.getFuncType())
        # Get shortest queue (global)
        shortest_idx, shortest_len = find_shortest_q(
            self.queue_length_tracking, starting_q=self.queue_index_offset
        )

        if found_affinity is True:
            # Pick shortest queue among those with affinity
            shortest_aff_idx, shortest_aff_len = find_shortest_q(
                self.queue_length_tracking, filter_list=queues_with_affinity
            )
            # Choose the global if affinity queue exceeds LB threshold (equal to JBSQ depth)
            if shortest_aff_len < self.load_balance_threshold:
                # Dispatch this thing based on affinity
                req.setAffinity()
                self.decisions_with_affinity += 1
                if affinity_in_input is True:
                    self.affinity_input_queue += 1
                else:
                    self.affinity_history_queue += 1
                if num_possible > 1:
                    self.decisions_with_multiple_affinity += 1
                final_dec = shortest_aff_idx
                assert shortest_aff_len < self.depth_limit
            else:
                # Fall back to load balance
                self.decisions_load_balance += 1
                final_dec = shortest_idx
                assert shortest_len < self.depth_limit
        else:
            # Dispatch to load balance by default
            self.decisions_load_balance += 1
            final_dec = shortest_idx
            assert shortest_len < self.depth_limit

        # Add this func to the length tracking meta-object
        self.queue_length_tracking[final_dec].appendleft(req)
        # print("Dispatched to core",final_dec,"queue",queue_string(self.queue_length_tracking[final_dec]))
        self.disp_decisions += 1

        self.queue_index_offset += 1
        if self.queue_index_offset >= self.num_queues:
            self.queue_index_offset = 0

        return final_dec
