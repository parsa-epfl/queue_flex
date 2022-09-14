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
from .end_measure import EndOfMeasurements
from simpy import Environment, Interrupt, Store, Event
from .dispatch_policies.base_policies import RandomDispatchPolicy, find_shortest_q
from .dispatch_policies.key_based_policies import KeyDispatchPolicy
from .dispatch_policies.JBSQ import JBSQDispatchPolicy, JBSQWithStatistics
from .comm_channel import (
    CommChannel,
    portable_iterate_queued_items,
    portable_get_q_depth,
)
from .dispatch_policies.util import queue_string
from .requests import AbstractRequest, RPCRequest, PullFeedbackRequest
from .request_filter import RequestFilter, QueuedRequestAnalyzerInterface
from .request_filter_lambdas import reqs_conflict
from .load_generator import OpenPoissonLoadGen
from .bucketed_index import BucketedIndex, is_odd
from .forked_pdb import ForkedPdb

# Python base package includes
from random import randint
from collections import Counter, deque
from functools import reduce
import typing
import copy


def hash_req_to_bucket(req: RPCRequest, bucket_mod: int) -> int:
    """Return the bucket number of an RPCRequest"""
    return hash(req) % bucket_mod


def get_queue_with_conflict(
    arg_dict: typing.Dict[int, typing.List[RPCRequest]],
    req_to_match: RPCRequest,
    bucket_mod: int,
) -> int:
    """Given a dictionary of queue->list of requests, return the queue idx with a conf.
    with the provided RPCRequest.
    This code preserves the \"Highlander Invariant\". There can be, only one.
    (Because of CREW).
    """
    num_matched = 0
    last_matched = -1
    for q_idx, req_list in arg_dict.items():
        if any(map(lambda x: reqs_conflict(x, req_to_match, bucket_mod), req_list)):
            num_matched += 1
            last_matched = q_idx

    assert num_matched == 1
    return last_matched


class LoadBalancer(QueuedRequestAnalyzerInterface):
    """
    A load balancer which takes requests from an input queue and puts them into
    output queues.

    This class serves as a load balancer which takes incoming objects (of any type)
    and puts them into to a set of queues passed to it.
    Various queueing policies are implementable by simply extending the DispatchPolicy
    subclass, which provide the \"selectQueue\" function.
    """

    def __init__(
        self,
        simpy_env,
        lgen_to_interrupt,
        in_queue,
        disp_queues,
        pull_queue=None,
        dp=None,
        is_parent=False,
    ):
        self.env = simpy_env
        self.in_q = in_queue
        self.worker_qs = disp_queues
        self.queues_including_processing_req = [
            deque() for i in range(len(disp_queues))
        ]
        self.lgen_to_interrupt = lgen_to_interrupt
        self.killed = False
        self.core_list = None
        self.num_times_blocked = 0
        self.pull_q = pull_queue
        if dp is None:
            self.dispatch_policy = RandomDispatchPolicy(len(disp_queues))
        else:
            self.dispatch_policy = dp

        # Data array with histogram of queue depth values
        self.private_q_histograms = []
        for i in range(len(self.worker_qs)):
            self.private_q_histograms.append(Counter())

        if not is_parent:
            self.action = self.env.process(self.run())

    def set_cores(self, corelist):
        self.core_list = corelist

    def endSimGraceful(self):
        try:
            self.lgen_to_interrupt.action.interrupt("end of sim")
        except RuntimeError as e:
            print("Caught exception", e, "lets transparently ignore it")
        self.killed = True

    def selectQueue(self, req):
        the_q_num = self.dispatch_policy.select(req)
        return the_q_num, self.worker_qs[the_q_num]

    def func_executed(self, qdx, f_type):
        self.queues_including_processing_req[qdx].pop()
        if callable(getattr(self.dispatch_policy, "func_executed", None)):
            self.dispatch_policy.func_executed(qdx, f_type)

    def wait_for_available_queue(self):
        event_on_avail = self.env.event()
        self.dispatch_policy.set_func_exec_event(event_on_avail)
        yield event_on_avail

    def update_pulls(self, pull_req):
        if pull_req.hasCompletedReq():
            self.func_executed(pull_req.getID(), pull_req.getCompletedReqType())
        else:
            self.func_executed(pull_req.getID(), 0)

    def request_in_pull_q(self, pq) -> int:
        if isinstance(pq, CommChannel):
            return pq.num_items_enqueued()
        else:  # standard simpy object
            return len(pq.items)

    def get_reqs_dispatched_to_q(self, q_num: int) -> int:
        """Overrides QueuedRequestAnalyzerInterface.get_reqs_dispatched_to_q(...)"""
        return self.dispatch_policy.get_reqs_dispatched_to_q(q_num)

    def get_reqs_in_private_q(self, q_num: int) -> int:
        """Overrides QueuedRequestAnalyzerInterface.get_reqs_in_private_q(...)"""
        return self.dispatch_policy.get_reqs_waiting_in_q(q_num)

    def num_reqs_in_private_qs(self):
        num_private_queues = len(self.worker_qs)
        return reduce(
            lambda x, y: x + y,
            [
                self.dispatch_policy.get_reqs_dispatched_to_q(i)
                for i in range(num_private_queues)
            ],
        )

    def histograms_for_core(self, core):
        return self.private_q_histograms[core]

    def iterate_private_queue(self, qdx, func):
        retlist = []
        for item in portable_iterate_queued_items(self.worker_qs[qdx]):
            if not isinstance(item, EndOfMeasurements):
                if func(item):
                    retlist.append(item)
        return qdx, retlist

    def iterate_private_queue_plusproc(self, qdx, func):
        retlist = []
        for item in portable_iterate_queued_items(
            self.queues_including_processing_req[qdx]
        ):
            if not isinstance(item, EndOfMeasurements):
                if func(item):
                    retlist.append(item)
        return qdx, retlist

    def search_queues_for_matching_requests(
        self,
        exclude_queue_list: typing.List[int],
        func_to_match: typing.Callable,
        include_processing_request: bool,
    ) -> typing.Dict[int, typing.List[AbstractRequest]]:
        """Iterate through all private queues minus those in the exclude queue list,
        and return a dict that contains all requests matching
        the func_to_match callable."""

        num_private_queues = len(self.worker_qs)
        all_indexes = range(num_private_queues)
        final_queue_indexes = [
            item for item in all_indexes if item not in exclude_queue_list
        ]
        if include_processing_request:
            per_queue_output = map(
                lambda x: self.iterate_private_queue_plusproc(x, func_to_match),
                final_queue_indexes,
            )
        else:
            per_queue_output = map(
                lambda x: self.iterate_private_queue(x, func_to_match),
                final_queue_indexes,
            )

        odict = {}
        for qdx, matching_list in per_queue_output:
            odict[qdx] = matching_list
        return odict

    def update_private_q_histograms(self):
        if isinstance(self.dispatch_policy, QueuedRequestAnalyzerInterface):
            for i in range(len(self.worker_qs)):
                num_dispatched = self.get_reqs_dispatched_to_q(i)
                self.private_q_histograms[i][num_dispatched] += 1

    def select_and_dispatch(self, req: RPCRequest):
        if isinstance(req, EndOfMeasurements):
            self.worker_qs[0].put(req)
            return
        queue_num, the_queue = self.selectQueue(req)
        assert queue_num != -1, "selectQueue in JBSQ returned all queues full!"
        req.dispatch_time = self.env.now
        the_queue.put(req)
        self.queues_including_processing_req[queue_num].appendleft(copy.deepcopy(req))

        # Notify cores of dispatch (if present)
        if self.core_list is not None:
            if callable(getattr(self.core_list[queue_num], "new_dispatch", None)):
                self.core_list[queue_num].new_dispatch(req.getFuncType())

    def run(self):
        while self.killed is False:
            # Statistics for private queue depths
            self.update_private_q_histograms()

            # Handle all outstanding pulls
            while self.request_in_pull_q(self.pull_q) > 0:
                pr = yield self.pull_q.get()
                self.update_pulls(pr)
                # print("Processing pull request at time {}".format(self.env.now),"State of per-core input queues:")
                # self.dispatch_policy.print_queues()

            if isinstance(self.dispatch_policy, JBSQDispatchPolicy):
                while self.dispatch_policy.no_queue_available():
                    # print("Blocked, no queues available at time {}".format(self.env.now),"State of per-core input queues:")
                    # self.dispatch_policy.print_queues()
                    self.num_times_blocked += 1
                    pr = yield self.pull_q.get()
                    self.update_pulls(pr)
                    # print("Load balancer UNBLOCKED at time {}".format(self.env.now),"State of per-core input queues:")
                    # self.dispatch_policy.print_queues()

            # Get next request from load generator
            req = yield self.in_q.get()
            self.select_and_dispatch(req)
            # print("Dispatching req",req,"at time",self.env.now)
            # print("Input queue length = ",portable_get_q_depth(self.in_q))
            # self.dispatch_policy.print_queues()


class IndexAwareLoadBalancer(LoadBalancer):
    """
    A load balancer which is aware of a BucketedIndex, and does not dispatch incoming requests that are to
    a bucket of the index which matches a particular criterion.
    """

    def __init__(
        self,
        simpy_env: Environment,
        lgen_to_interrupt: OpenPoissonLoadGen,
        in_queue: CommChannel,
        disp_queues: typing.List[CommChannel],
        index_obj: BucketedIndex,
        pull_queue: CommChannel = None,
        dp: typing.Union[KeyDispatchPolicy, JBSQDispatchPolicy] = None,
    ) -> None:
        super().__init__(
            simpy_env, lgen_to_interrupt, in_queue, disp_queues, pull_queue, dp
        )
        self.index_obj = index_obj
        self.blocked_queues = [deque() for i in range(self.index_obj.get_num_buckets())]

        # Critical to override self.action
        self.action = self.env.process(self.run())

        # Event for blocking on JBSQ
        self.jbsq_event = None

    def check_queue_invariant(
        self, req_conflicted_with: RPCRequest, q_idx: int
    ) -> None:
        """Verify the invariant that all reqs in a specific self.blocked_queue are to the same index, and that
        they would currently create a conflict if dispatched."""
        num_buckets = self.index_obj.get_num_buckets()
        for req in self.blocked_queues[q_idx]:
            assert hash_req_to_bucket(req, num_buckets) == hash_req_to_bucket(
                req_conflicted_with, num_buckets
            ), "Request ID {} to bucket {} that is currently blocked, does NOT match incoming request {} to bucket {}.".format(
                req.getID(),
                hash(req),
                req_conflicted_with.getID(),
                hash(req_conflicted_with),
            )
            does_conflict, conf_map = self.causes_conflict(req)
            for qdx, q_reqs in conf_map.items():
                for currently_queued_conflict in q_reqs:
                    assert hash_req_to_bucket(req, num_buckets) == hash_req_to_bucket(
                        currently_queued_conflict, num_buckets
                    ), "Request ID {} to bucket {} that is currently blocked, does NOT conflict with currently queued request {} to bucket {}.".format(
                        req.getID(),
                        hash_req_to_bucket(req, num_buckets),
                        currently_queued_conflict.getID(),
                        hash_req_to_bucket(currently_queued_conflict, num_buckets),
                    )

    def dispatch_to_q(self, req: RPCRequest, q_idx: int) -> None:
        """Dispatch the provided req directly to the provided queue index."""
        req.dispatch_time = self.env.now
        self.worker_qs[q_idx].put(req)
        self.queues_including_processing_req[q_idx].appendleft(copy.deepcopy(req))
        if callable(getattr(self.dispatch_policy, "notify_dispatch", None)):
            self.dispatch_policy.notify_dispatch(q_idx, req)

    def select_and_dispatch_from_blocked_q(
        self,
        bucket: int,
        # event: Event,
    ) -> None:
        """Dispatch blocked reqs in the waiting queue indicated by the event argument, up to the next write."""
        # (bucket, unlocked_version) = event.value
        while self.blocked_queues[bucket]:
            req = self.blocked_queues[bucket].pop()
            queues_full = False
            if isinstance(self.dispatch_policy, JBSQDispatchPolicy):
                queues_full = self.dispatch_policy.no_queue_available()
                self.num_times_blocked += 1

            will_conflict, conf_map = self.causes_conflict(req)
            if not will_conflict and not queues_full:
                # print("Dispatching req ID from blocked queues",req.getID())
                # self.dispatch_policy.print_queues()
                self.select_and_dispatch(req)
            else:
                self.serialize_req(req, bucket, at_tail=False)  # back at head
                break

    def serialize_req(
        self,
        req: RPCRequest,
        bucket: int,
        at_tail: bool = True,
    ) -> None:
        """Serialize this request on the provided bucket so it dispatches after."""
        if at_tail:
            self.blocked_queues[bucket].appendleft(req)
        else:
            self.blocked_queues[bucket].append(req)
        # print("Serialized req",req.getID(),"in queue",bucket,"queue now looks like")
        # for x in self.blocked_queues[bucket]:
        # print("Req",x.getID(),"to bucket",hash(x) % self.index_obj.get_num_buckets())

    def causes_conflict(
        self,
        req: RPCRequest,
    ) -> bool:
        """Return true if the provided request will conflict on the provided bucket."""
        if isinstance(req, EndOfMeasurements):
            return False

        def wrapper_single_req(another_req: RPCRequest) -> bool:
            """Wrapper that calls reqs_conflict with the request provided to \'causes_conflict\'
            and the bucket count given in this index.
            """
            return reqs_conflict(req, another_req, self.index_obj.get_num_buckets())

        # print("Process checking for conflicts for req",req.getID())
        conflicting_reqs = self.search_queues_for_matching_requests(
            [],  # exclude no queues,
            wrapper_single_req,  # callable
            include_processing_request=True,
        )
        # print("Found",conflicting_reqs)

        """
        for qdx, q_reqs in conflicting_reqs.items():
            if len(q_reqs) > 0:
                print("Queue index",qdx,"has conflicting requests.")
                for r in q_reqs:
                    print("Request",r.getID(),"conflicts")
                return True, conflicting_reqs
        """
        it = map(lambda x: len(x) > 0, conflicting_reqs.values())
        if any(it):
            return True, conflicting_reqs
        return False, conflicting_reqs

    def update_pulls(
        self,
        pull_req: PullFeedbackRequest,
    ) -> None:
        """
        Use the metadata in the provided pull_req to update private queue tracking
        and trigger func_executed.

        Uses the superclass to update the metadata, and then adds the bucket-based
        dispatch onto that through the interface \"select_and_dispatch_from_blocked_q\".
        """
        super().update_pulls(pull_req)
        bucket_completed = hash_req_to_bucket(
            pull_req.req_completed, self.index_obj.get_num_buckets()
        )
        # print("Process dispatching from pull queue on bucket",bucket_completed)
        # print("Updating pull, req ID {} finished.".format(pull_req.req_completed.getID()))
        # self.dispatch_policy.print_queues()
        self.select_and_dispatch_from_blocked_q(bucket_completed)

    def pull_queue_updater(self):
        """Process running independently to update the pull queue"""
        while self.killed is False:
            pr = yield self.pull_q.get()
            self.update_pulls(pr)
            if self.jbsq_event is not None:
                self.jbsq_event.succeed()
                self.jbsq_event = None

    def run(self):
        """
        Function repeatedly called by simpy. Overrides the parent LoadBalancer class\'
        self.action parameter.
        """
        # Pulls handled by subprocess outside of loop
        self.updater = self.env.process(self.pull_queue_updater())
        while self.killed is False:
            # Statistics for private queue depths
            self.update_private_q_histograms()

            req = yield self.in_q.get()

            if isinstance(self.dispatch_policy, JBSQDispatchPolicy):
                while self.dispatch_policy.no_queue_available() and not req.getWrite():
                    self.num_times_blocked += 1
                    # Arm jbsq event and wait
                    self.jbsq_event = self.env.event()
                    yield self.jbsq_event

            bucket = hash(req) % self.index_obj.get_num_buckets()

            if isinstance(req, EndOfMeasurements):
                self.worker_qs[0].put(req)
                continue

            """
            if not req.getWrite():
                will_conflict, conf_req_map = self.causes_conflict(req)
                if will_conflict:
                    # Get the queue with the conflict, send this read there.
                    queue_idx_to_dispatch = get_queue_with_conflict(
                            conf_req_map,
                            req,
                            self.index_obj.get_num_buckets()
                        )
                    assert queue_idx_to_dispatch != -1
                    self.dispatch_to_q(req,queue_idx_to_dispatch)
                    continue

            # Otherwise, was not an interesting conflict
            self.select_and_dispatch(req)
            """
            will_conflict, conf_req_map = self.causes_conflict(req)
            if will_conflict or self.blocked_queues[bucket]:
                self.serialize_req(req, bucket)
                # self.check_queue_invariant(req, bucket) # SUPER COSTLY, do not enable unless sure.
            else:
                # print("Dispatching req",req.getID())
                # self.dispatch_policy.print_queues()
                self.select_and_dispatch(req)


class DynamicEWLoadBalancer(LoadBalancer):
    """
    A load balancer which has a local data structure tracking bucket->core exclusive mappings, so that
    writes can have a modicum of load balancing (e.g., if there happens to be an idle core).
    """

    def __init__(
        self,
        simpy_env: Environment,
        lgen_to_interrupt: OpenPoissonLoadGen,
        in_queue: CommChannel,
        disp_queues: typing.List[CommChannel],
        index_obj: BucketedIndex,
        pull_queue: CommChannel = None,
        dp: typing.Union[KeyDispatchPolicy, JBSQDispatchPolicy] = None,
    ) -> None:
        super().__init__(
            simpy_env,
            lgen_to_interrupt,
            in_queue,
            disp_queues,
            pull_queue,
            dp,
            is_parent=True,  # Don't instantiate another runner
        )
        self.index_obj = index_obj

        # Critical to override self.action
        self.action = self.env.process(self.run())

        # Event for blocking on JBSQ
        self.jbsq_event = None

    def check_queue_invariant(
        self, req_conflicted_with: RPCRequest, q_idx: int
    ) -> None:
        """Verify the invariant that all reqs in a specific self.blocked_queue are to the same index, and that
        they would currently create a conflict if dispatched."""
        num_buckets = self.index_obj.get_num_buckets()
        for req in self.blocked_queues[q_idx]:
            assert hash_req_to_bucket(req, num_buckets) == hash_req_to_bucket(
                req_conflicted_with, num_buckets
            ), "Request ID {} to bucket {} that is currently blocked, does NOT match incoming request {} to bucket {}.".format(
                req.getID(),
                hash(req),
                req_conflicted_with.getID(),
                hash(req_conflicted_with),
            )
            does_conflict, conf_map = self.causes_conflict(req)
            for qdx, q_reqs in conf_map.items():
                for currently_queued_conflict in q_reqs:
                    assert hash_req_to_bucket(req, num_buckets) == hash_req_to_bucket(
                        currently_queued_conflict, num_buckets
                    ), "Request ID {} to bucket {} that is currently blocked, does NOT conflict with currently queued request {} to bucket {}.".format(
                        req.getID(),
                        hash_req_to_bucket(req, num_buckets),
                        currently_queued_conflict.getID(),
                        hash_req_to_bucket(currently_queued_conflict, num_buckets),
                    )

    def dispatch_to_q(self, req: RPCRequest, q_idx: int) -> None:
        """Dispatch the provided req directly to the provided queue index."""
        req.dispatch_time = self.env.now
        self.worker_qs[q_idx].put(req)
        self.queues_including_processing_req[q_idx].appendleft(copy.deepcopy(req))
        if callable(getattr(self.dispatch_policy, "notify_dispatch", None)):
            self.dispatch_policy.notify_dispatch(q_idx, req)

    def update_pulls(
        self,
        pull_req: PullFeedbackRequest,
    ) -> None:
        """
        Use the metadata in the provided pull_req to update private queue tracking
        and trigger func_executed.

        Uses the superclass to update the metadata, and then adds the bucket-based
        dispatch onto that through the interface \"write_req_finished\".
        """
        super().update_pulls(pull_req)
        bucket_completed = hash_req_to_bucket(
            pull_req.req_completed, self.index_obj.get_num_buckets()
        )
        # print("Process dispatching from pull queue on bucket",bucket_completed)
        """
        print(
            "Updating pull, req ID {} finished.".format(pull_req.req_completed.getID())
        )
        """
        # self.dispatch_policy.print_queues()
        if pull_req.req_completed.getWrite():
            # print("Updating write excl_q metadata.")
            self.dispatch_policy.write_req_finished(bucket_completed, pull_req.getID())

    def pull_queue_updater(self):
        """Process running independently to update the pull queue"""
        while self.killed is False:
            pr = yield self.pull_q.get()
            self.update_pulls(pr)
            if self.jbsq_event is not None:
                self.jbsq_event.succeed()
                self.jbsq_event = None

    def run(self):
        """
        Function repeatedly called by simpy. Overrides the parent LoadBalancer class\'
        self.action parameter.
        """
        # Pulls handled by subprocess outside of loop
        # self.updater = self.env.process(self.pull_queue_updater())
        while self.killed is False:
            # Statistics for private queue depths
            self.update_private_q_histograms()

            # Handle all outstanding pulls
            while self.request_in_pull_q(self.pull_q) > 0:
                pr = yield self.pull_q.get()
                # print("Load balancer got pull message at time {}".format(self.env.now))
                self.update_pulls(pr)

            if isinstance(self.dispatch_policy, JBSQDispatchPolicy):
                while self.dispatch_policy.no_queue_available():
                    self.num_times_blocked += 1
                    pr = yield self.pull_q.get()
                    # print("Load balancer got pull message at time {}".format(self.env.now))
                    self.update_pulls(pr)
                    # Arm jbsq event and wait
                    # self.jbsq_event = self.env.event()
                    # yield self.jbsq_event

            # Unblocked, get next req and dispatch
            req = yield self.in_q.get()
            self.select_and_dispatch(req)
            # print("Dispatching req",req,"at time",self.env.now)
