
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
# Author: Mark Sutherland, (C) 2021
from simpy import Store, Event, Environment

from .base_policies import find_shortest_q, helper_get_q_depth
from .key_based_policies import DispatchNotificationInterface
from ..requests import FuncRequest, AbstractRequest
from collections import deque, Counter, OrderedDict
from .util import func_type_portable, queue_string
from ..request_filter import QueuedRequestAnalyzerInterface, RequestFilter
from ..comm_channel import portable_get_q_depth, CommChannel
from ..forked_pdb import ForkedPdb
from enum import Enum, unique, auto

import typing


class JBSQDispatchPolicy(QueuedRequestAnalyzerInterface):
    def __init__(self, env, qs, bound):
        if bound == 0:
            raise ValueError("Cannot have JBSQ(0)")

        self.env = env
        self.queues = qs
        self.depth_limit = bound
        self.func_executed_event = None

        self.num_queues = len(self.queues)
        # all queues start with zero elements
        self.queue_length_tracking = [deque() for i in range(self.num_queues)]

    def print_queues(self) -> None:
        """Print out the queue tracking objects."""
        for i in range(self.num_queues):
            print(
                "Queue {} is: {}".format(i, queue_string(self.queue_length_tracking[i]))
            )

    # Setup an event which is triggered by a function being executed
    def set_func_exec_event(self, event: Event) -> None:
        self.func_executed_event = event

    def func_executed(self, c_id: int, f_type: int) -> None:
        # print("Queue id {} had func_executed event!".format(c_id))
        self.queue_length_tracking[c_id].pop()
        # print("Queue state is now:",queue_string(self.queue_length_tracking[c_id]))
        if self.func_executed_event is not None:
            # print('Triggering event executed from core',c_id,'func type',f_type,'at time',self.env.now)
            self.func_executed_event.succeed()
            self.func_executed_event = None

    # Return true if all private queues are equal to or greater than the depth limit.
    # JBSQ disallows dispatch in this case
    def no_queue_available(self) -> bool:
        shortest_idx, shortest_len = find_shortest_q(self.queue_length_tracking)
        if shortest_len >= self.depth_limit:
            return True
        else:
            return False

    def select(self, req: AbstractRequest) -> int:
        shortest_idx, shortest_len = find_shortest_q(self.queue_length_tracking)
        if shortest_len < self.depth_limit:
            self.queue_length_tracking[shortest_idx].appendleft(req)
            return shortest_idx
        else:
            return -1

    def get_reqs_dispatched_to_q(self, q_num: int) -> int:
        """Overrides QueuedRequestAnalyzerInterface.get_reqs_dispatched_to_q(...)"""
        return len(self.queue_length_tracking[q_num])

    def get_reqs_in_private_q(self, q_num: int) -> int:
        """Overrides QueuedRequestAnalyzerInterface.get_reqs_in_private_q(...)"""
        return portable_get_q_depth(self.queues[q_num])

    def filter_reqs_in_private_q(self, q_num: int, f: RequestFilter):
        """Overrides QueuedRequestAnalyzerInterface.filter_reqs_in_private_q(...)"""
        return []


class JBSQWithStatistics(JBSQDispatchPolicy):
    """
    This class performs JBSQ dispatch with additional statistics.

    A class which adds statistics to the baseline JBSQ. It records:
    - total number of dispatch decisions
    - for each dispatch decision, was it possible to choose a queue with 'affinity'
    - a sample of all of the dispatch queue lengths over the duration of the simulation
    This class only works with requests of class FuncRequest, as it uses "getFuncType"
    """

    def __init__(self, env, qs, bound, sample_interval=1, thresh=1):
        super().__init__(env, qs, bound)
        self.sample_interval = sample_interval
        self.reset_val = sample_interval

        self.disp_decisions = 0
        self.decisions_with_affinity = 0
        self.decisions_with_multiple_affinity = 0
        self.qlen_sample = list()
        self.long_req_depth = list()
        self.queue_index_offset = 0
        self.num_queues = len(qs)
        self.MAX_HISTORY_LENGTH = 2
        self.affinity_threshold = thresh

        # input queues [ F0, F1 ]  -> C -> history queues [ F1, F1 ]
        self.history_queues = list()

        for i in range(self.num_queues):
            self.qlen_sample.append(list())
            self.history_queues.append(deque(maxlen=self.MAX_HISTORY_LENGTH))

    def func_executed(self, c_id, f_type):
        # print("Core",c_id,"executed function w. type",f_type)
        req = self.queue_length_tracking[c_id].pop()
        assert (
            func_type_portable(req) == f_type
        ), "Function popped from queue_length_tracking of type {} was not equal to f_type {} that was returned by the core".format(
            func_type_portable(req), f_type
        )
        # print("Input queue is now",queue_string(self.queue_length_tracking[c_id]))
        self.history_queues[c_id].appendleft(req)
        if self.func_executed_event is not None:
            # print('Triggering event executed from core',c_id,'func type',f_type,'at time',self.env.now)
            self.func_executed_event.succeed()
            self.func_executed_event = None

    def get_statistics(self):
        qlen_counter = Counter(self.long_req_depth)
        return {
            "disp_decisions": self.disp_decisions,
            "decisions_w_affinity": self.decisions_with_affinity,
            "decisions_w_multiple_affinity": self.decisions_with_multiple_affinity,
            "qlen_sample": qlen_counter,
        }

    def record_q_lengths(self):
        for qdx in range(self.num_queues):
            qlen = len(self.queue_length_tracking[qdx])
            self.qlen_sample[qdx].append(qlen)

    def get_num_long_reqs(self):
        long_req_ids = [0, 1]
        num_long = 0
        for q in self.queue_length_tracking:
            for rpc in q:
                if func_type_portable(rpc) in long_req_ids:
                    num_long += 1
        return num_long

    def scan_q(self, q, f_type, unique_funcs):
        found_affinity = False
        for rpc in q:
            if len(unique_funcs) > self.affinity_threshold:
                break
            if isinstance(
                rpc, FuncRequest
            ):  # list of FuncRequests with func type attribute
                if rpc.getFuncType() == f_type:
                    # print('HIT on type',rpc.getFuncType())
                    found_affinity = True
                    break
                # print('miss on type',rpc.getFuncType())
                if rpc.getFuncType() not in unique_funcs:
                    unique_funcs.append(rpc.getFuncType())
                # print('unique funcs is now',unique_funcs)
            else:  # list of func_ids
                # print('miss on type',rpc)
                if rpc == f_type:
                    # print('HIT on type',rpc)
                    found_affinity = True
                    break
                if rpc not in unique_funcs:
                    unique_funcs.append(rpc)
                # print('unique funcs is now',unique_funcs)
        return found_affinity, unique_funcs

    def search_affinity(self, f_type):
        found_affinity = False
        num_found = 0
        unique_funcs = list()
        queues_with_affinity = set()
        for i in range(self.num_queues):
            unique_funcs.append(list())

        for i in range(self.num_queues):
            # print('looking for affinity to func',f_type,'in input queue idx',i)
            # print('reversed:',list(self.queue_length_tracking[i]))
            success, unique_funcs[i] = self.scan_q(
                self.queue_length_tracking[i], f_type, unique_funcs[i]
            )
            if success is True:
                num_found += 1
                found_affinity = True
                queues_with_affinity.add(i)

        for i in range(len(self.history_queues)):
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

        return found_affinity, num_found

    def select(self, req):
        # Take statistics if interval expired
        self.sample_interval -= 1
        if self.sample_interval == 0:
            self.record_q_lengths()
            self.sample_interval = self.reset_val

        # Check affinity possible
        found_affinity, num_possible = self.search_affinity(req.getFuncType())
        if found_affinity is True:
            self.decisions_with_affinity += 1
        if num_possible > 1:
            self.decisions_with_multiple_affinity += 1

        # Do dispatch decision
        shortest_idx, shortest_len = find_shortest_q(
            self.queue_length_tracking, starting_q=self.queue_index_offset
        )
        assert (
            shortest_len < self.depth_limit
        )  # JBSQ must always check q is free before dispatch

        # Add req to queue length tracking meta-object
        # print("Dispatching func with id",req.getFuncType(),"to queue",shortest_idx)
        self.queue_length_tracking[shortest_idx].appendleft(req)

        self.disp_decisions += 1
        self.queue_index_offset += 1
        if self.queue_index_offset >= self.num_queues:
            self.queue_index_offset = 0
        return shortest_idx


class JBSCREWDispatchPolicy(JBSQDispatchPolicy):
    """
    This class implements CREW on top of JBSQ. If the queue depth is small enough, then
    dispatch based on CREW. Otherwise, return failure.
    """

    def __init__(
        self,
        simpy_env: Environment,
        queue_objects: typing.List[typing.Union[Store, CommChannel]],
        bound_depth: int,
        num_hash_buckets: int,
    ) -> None:
        super().__init__(simpy_env, queue_objects, bound_depth)
        self.num_hash_buckets = num_hash_buckets
        self.bucket_load_counter = Counter()

    def update_bucket_load(self, req: AbstractRequest) -> None:
        """Update the stat counter for bucket load."""
        bucket_number = hash(req) % self.num_hash_buckets
        self.bucket_load_counter[bucket_number] += 1

    def select(self, req: AbstractRequest) -> int:
        """Picks a queue for this request."""
        if req.getWrite():  # change queue idx and length to the partitioned queue
            bucket = hash(req) % self.num_hash_buckets
            qdx = bucket % self.num_queues
            q_len = helper_get_q_depth(self.queue_length_tracking[qdx])
            # print("JBSCREW: WRITE req ID",req.getID(),"selected queue",qdx,"that has length",q_len)
            self.queue_length_tracking[qdx].appendleft(req)
            self.update_bucket_load(req)
            return qdx
        else:
            qdx, q_len = find_shortest_q(self.queue_length_tracking)
            if q_len < self.depth_limit:  # dispatch, there is enough space
                # print("JBSCREW: READ req ID",req.getID(),"selected queue",qdx,"that has length",q_len)
                self.queue_length_tracking[qdx].appendleft(req)
                self.update_bucket_load(req)
                return qdx
            else:
                # print("Request {} (was write? {}) failed to dispatch because queue was full! Proposed queue and length: {} len. {}".format(req.getID(),req.getWrite(),qdx,q_len))
                return -1


class BucketMappingMetadata:
    """
    A class which stores metadata about which core is currently exclusively accessing this
    bucket, and how many write accesses are outstanding. Used by a dynamic load balancer.
    """

    def __init__(self, core_num: int, wrs_outstanding: int) -> None:
        self.core = core_num
        self.wrs_outstanding = wrs_outstanding

    def get_core_num(self) -> int:
        return self.core

    def get_outstanding_writes(self) -> int:
        return self.wrs_outstanding

    def dec_outstanding_writes(self) -> int:
        self.wrs_outstanding -= 1
        return self.wrs_outstanding

    def inc_outstanding_writes(self) -> int:
        self.wrs_outstanding += 1
        return self.wrs_outstanding


@unique
class ExclDispatchDecisionEnum(Enum):
    LB_READ = (auto(),)
    LIN_READ = (auto(),)
    NEW_EXCL_WRITE = (auto(),)
    FOLLOWING_EXCL_WRITE = (auto(),)


class DynJBSCREWDispatchPolicy(JBSQDispatchPolicy):
    """
    This class implements CREW on top of JBSQ, with an additional data structure that
    stores bucket->queue mappings. As soon as a write goes to a queue, it becomes the
    exclusive owner of that bucket, and only gets released when all writes to that
    queue are finished.
    """

    def __init__(
        self,
        simpy_env: Environment,
        queue_objects: typing.List[typing.Union[Store, CommChannel]],
        bound_depth: int,
        num_hash_buckets: int,
        max_buckets_tracking: int,
    ) -> None:
        super().__init__(simpy_env, queue_objects, bound_depth)
        self.bucket_mappings = OrderedDict()
        self.num_hash_buckets = num_hash_buckets
        self.max_buckets_tracking = max_buckets_tracking

        # Statistics
        self.balanced_writes = 0
        self.excl_writes = 0
        self.balanced_reads = 0
        self.linearized_reads = 0
        self.bucket_load_counter = Counter()

    def update_bucket_load(self, req: AbstractRequest) -> None:
        """Update the stat counter for bucket load."""
        bucket_number = hash(req) % self.num_hash_buckets
        self.bucket_load_counter[bucket_number] += 1

    def get_wr_statistics(self) -> typing.Dict[str, float]:
        total = self.balanced_writes + self.excl_writes
        if total:
            percent_balanced = float(self.balanced_writes) / total
            percent_excl = float(self.excl_writes) / total
        else:
            percent_balanced = percent_excl = 0
        return {"balanced": percent_balanced, "exclusive": percent_excl}

    def get_read_statistics(self) -> typing.Dict[str, float]:
        """Return statistics for the fraction of balanced and linearized reads."""
        total = self.balanced_reads + self.linearized_reads
        if total:
            percent_balanced = float(self.balanced_reads) / total
            percent_lin = float(self.linearized_reads) / total
        else:
            percent_balanced = percent_lin = 0
        return {"balanced": percent_balanced, "linearized": percent_lin}

    def update_q_len_tracking(self, qdx: int, req: AbstractRequest) -> None:
        """Update queue length tracking metadata object."""
        self.queue_length_tracking[qdx].appendleft(req)

    def add_to_excl_bucket(
        self,
        bucket: int,
        excl_q: int,
    ) -> None:
        """Create exclusive association between bucket and excl_q, so that all reqs with
        the same matching hash go to the same queue, until the mapping is released."""
        if len(self.bucket_mappings) >= self.max_buckets_tracking:
            # remove the oldest one
            self.bucket_mappings.popitem(last=False)  # FIFO parameter means LRU
        self.bucket_mappings[bucket] = BucketMappingMetadata(excl_q, 1)
        # print("excl q mapping for bucket {} is -> {}".format(bucket, excl_q))

    def write_req_finished(
        self,
        bucket: int,
        excl_q: int,
    ) -> int:
        """Decrement the number of write reqs on bucket, and if the number is zero, break
        the link between bucket and excl_q. Return the new number of write reqs on bucket."""
        assert (
            bucket in self.bucket_mappings
        ), "Bucket {} not in exclusive mappings despite having a write req finished for it!".format(
            bucket
        )

        """
        print(
            "LB reports write req finished on excl q {}, that is mapped to bucket {}. Bucket mapping object reports mapped to q {}".format(
                excl_q, bucket, self.bucket_mappings[bucket].get_core_num()
            )
        )
        """
        cur_assigned_q = self.bucket_mappings[bucket].get_core_num()
        assert (
            cur_assigned_q == excl_q
        ), "Bucket {} was currently assigned excl to queue {}, but a write req finished on queue {}".format(
            bucket, cur_assigned_q, excl_q
        )

        new_outstanding = self.bucket_mappings[bucket].dec_outstanding_writes()
        if new_outstanding == 0:
            del self.bucket_mappings[bucket]
        return new_outstanding

    def exclusive_access_outstanding(
        self,
        req: AbstractRequest,
    ) -> bool:
        """Check to see if this request would go to a bucket that has a core holding exclusive access on it."""
        bucket = hash(req) % self.num_hash_buckets
        if bucket in self.bucket_mappings:
            return True
        else:
            return False

    def update_metadata(
        self,
        req: AbstractRequest,
        decision: ExclDispatchDecisionEnum,
        queue_chosen: int,
        bucket: int,
    ) -> None:
        """Update the exclusive metadata table and stats for this request, given a particular dispatch decision."""
        if decision is ExclDispatchDecisionEnum.LB_READ:
            self.balanced_reads += 1
        elif decision is ExclDispatchDecisionEnum.LIN_READ:
            self.linearized_reads += 1
        elif decision is ExclDispatchDecisionEnum.FOLLOWING_EXCL_WRITE:
            self.bucket_mappings[bucket].inc_outstanding_writes()
            self.excl_writes += 1
        elif decision is ExclDispatchDecisionEnum.NEW_EXCL_WRITE:
            self.add_to_excl_bucket(bucket, queue_chosen)
            self.balanced_writes += 1
        self.update_q_len_tracking(queue_chosen, req)
        self.update_bucket_load(req)

    def select(self, req: AbstractRequest) -> int:
        """Picks a queue for this request."""
        bucket = hash(req) % self.num_hash_buckets
        if self.exclusive_access_outstanding(req):
            #print("Got here, excl outstanding for bucket {}".format(bucket))
            if req.getWrite():
                # Dispatch to the exclusive queue.
                disp_q = self.bucket_mappings[bucket].get_core_num()
                self.update_metadata(
                    req, ExclDispatchDecisionEnum.FOLLOWING_EXCL_WRITE, disp_q, bucket
                )
                #print("Write already-exclusive to bucket {}, core {}".format(bucket,disp_q))
            else:  # Read
                disp_q, q_len = find_shortest_q(self.queue_length_tracking)
                if q_len < self.depth_limit:
                    self.update_metadata(
                        req, ExclDispatchDecisionEnum.LB_READ, disp_q, bucket
                    )
                else:
                    disp_q = -1
                #print("Read request w/ outstanding write to bucket {}, picked shortest_q core {}.".format(bucket,disp_q))
            return disp_q
        else:
            # Find shortest queue, load balance this request
            disp_q, q_len = find_shortest_q(self.queue_length_tracking)
            if q_len < self.depth_limit:
                if req.getWrite():
                    #print("Adding new exclusive access metadata for bucket {}, core {}".format(bucket,disp_q))
                    # Add to exclusive access.
                    self.update_metadata(
                        req, ExclDispatchDecisionEnum.NEW_EXCL_WRITE, disp_q, bucket
                    )
                else:
                    #print("Normal load balanced read to bucket {}, core {}".format(bucket,disp_q))
                    # Nothing to do here, it's a normal load balanced read.
                    self.update_metadata(
                        req, ExclDispatchDecisionEnum.LB_READ, disp_q, bucket
                    )
            else:
                disp_q = -1
            return disp_q
