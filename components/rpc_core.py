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
from .serv_times.exp_generator import ExpServTimeGenerator
from .requests import PullFeedbackRequest, FuncRequest, RPCRequest, AbstractRequest
from .request_filter_lambdas import (
    req_is_read,
    read_to_different_bucket,
    younger_read_to_different_bucket,
)
from .end_measure import EndOfMeasurements
from .bucketed_index import BucketedIndex, is_odd
from .comm_channel import CommChannel
from .latency_store import LatencyStoreWithBreakdown
from .load_generator import AbstractLoadGen
from .load_balancer import LoadBalancer
from .request_filter_lambdas import req_is_read, read_to_different_bucket
from .cache_state import PrivateDataCache, KVPair
from .cache_snoop_interface import CacheSnoopInterface
from numpy.random import exponential
from collections import Counter

import simpy
import typing
import copy


class AbstractCore(object):
    def __init__(self, core_id, lgen_to_interrupt):
        # Used for calculating service stability
        self.lgen_to_interrupt = lgen_to_interrupt
        self.kill_sim_threshold = 1000000
        self.lastFiveSTimes = []
        if core_id == 0:
            self.isMaster = True
        else:
            self.isMaster = False

    def putSTime(self, time):
        self.lastFiveSTimes.append(time)
        if len(self.lastFiveSTimes) >= 5:
            del self.lastFiveSTimes[0]

    def checkTimeOverThreshold(self, item):
        if item >= self.kill_sim_threshold:
            return True
        return False

    def isSimulationUnstable(self):
        timeGreaterThanThresholdList = [
            self.checkTimeOverThreshold(x) for x in self.lastFiveSTimes
        ]
        if all(timeGreaterThanThresholdList) is True:
            return True
        return False

    def endSimGraceful(self):
        try:
            self.lgen_to_interrupt.action.interrupt("end of sim")
        except RuntimeError as e:
            print("Caught exception", e, "lets transparently ignore it")
        self.killed = True

    def check_req_end_sim(self, rpc: AbstractRequest) -> bool:
        """Return true if provided req is an EndOfMeasurements type, indicating that
        the simulation is over and was killed.
        """
        if isinstance(rpc, EndOfMeasurements):
            self.endSimGraceful()
            return True
        return False

    def endSimUnstable(self):
        if self.isMaster is True:
            try:
                self.lgen_to_interrupt.action.interrupt("unstable")
            except RuntimeError as e:
                print("Caught exception", e, "lets transparently ignore it")
        self.killed = True


class uServCore(AbstractCore):
    def __init__(
        self,
        simpy_env,
        core_id,
        request_queue,
        measurement_store,
        stime_gen,
        lgen_to_interrupt,
        load_balancer=None,
        pull_queue=None,
    ):
        super().__init__(core_id, lgen_to_interrupt)
        self.env = simpy_env
        self.id = core_id
        self.in_q = request_queue
        self.latency_store = measurement_store
        self.stime_gen = stime_gen
        self.killed = False
        self.action = self.env.process(self.run())
        self.numSimulated = 0
        self.lb = load_balancer
        self.pull_queue = pull_queue

    def run(self):
        while self.killed is False:
            rpc = yield self.in_q.get()

            rpcNumber = rpc.num
            rpc.start_proc_time = self.env.now

            # Model a service time
            if isinstance(rpc, FuncRequest):
                yield self.env.timeout(self.stime_gen.get(rpc.getFuncType()))
            else:
                yield self.env.timeout(self.stime_gen.get())

            # RPC is done
            rpc.end_proc_time = self.env.now
            rpc.completion_time = (
                self.env.now
            )  # This may need to be changed to model any "end of rpc" actions
            total_time = rpc.getTotalServiceTime()
            self.latency_store.record_value(total_time)
            self.putSTime(total_time)

            if self.isMaster is True and self.isSimulationUnstable() is True:
                print(
                    "Simulation was unstable, last five service times from core 0 were:",
                    self.lastFiveSTimes,
                    ", killing sim.",
                )
                self.endSimUnstable()
            self.numSimulated += 1
            if self.pull_queue is not None:
                self.pull_queue.put(PullFeedbackRequest(self.id, rpc))
            else:
                self.lb.func_executed(self.id, rpc.getFuncType())


class BimodaluServCore(AbstractCore):
    def __init__(
        self,
        simpy_env,
        core_id,
        request_queue,
        measurement_store,
        stime_gen_hit,
        stime_gen_miss,
        lgen_to_interrupt,
        load_balancer=None,
        pull_queue=None,
    ):
        super().__init__(core_id, lgen_to_interrupt)
        self.env = simpy_env
        self.id = core_id
        self.in_q = request_queue
        self.latency_store = measurement_store
        self.hit_generator = stime_gen_hit
        self.miss_generator = stime_gen_miss
        self.killed = False
        self.action = self.env.process(self.run())
        self.numSimulated = 0
        self.lb = load_balancer
        self.pull_queue = pull_queue

    def run(self):
        while self.killed is False:
            rpc = yield self.in_q.get()

            rpcNumber = rpc.num
            rpc.start_proc_time = self.env.now

            # Model a service time
            if rpc.affinityHit():
                yield self.env.timeout(self.hit_generator.get())
            else:
                yield self.env.timeout(self.miss_generator.get())

            # RPC is done
            rpc.end_proc_time = self.env.now
            rpc.completion_time = (
                self.env.now
            )  # This may need to be changed to model any "end of rpc" actions
            total_time = rpc.getTotalServiceTime()
            self.latency_store.record_value(total_time)
            self.putSTime(total_time)

            if self.isMaster is True and self.isSimulationUnstable() is True:
                print(
                    "Simulation was unstable, last five service times from core 0 were:",
                    self.lastFiveSTimes,
                    ", killing sim.",
                )
                self.endSimUnstable()
            self.numSimulated += 1

            if self.pull_queue is not None:
                self.pull_queue.put(PullFeedbackRequest(self.id, rpc))
            else:
                self.lb.func_executed(self.id, rpc.getFuncType())


class ReadWriteRPCCore(AbstractCore):
    def __init__(
        self,
        simpy_env,
        core_id,
        request_queue,
        measurement_store,
        rd_gen,
        wr_gen,
        lgen_to_interrupt,
        pull_queue=None,
    ):
        super().__init__(core_id, lgen_to_interrupt)
        self.env = simpy_env
        self.id = core_id
        self.in_q = request_queue
        self.latency_store = measurement_store
        self.read_distribution_generator = rd_gen
        self.write_distribution_generator = wr_gen
        self.killed = False
        self.action = self.env.process(self.run())
        self.numSimulated = 0
        self.pull_queue = pull_queue

    def run(self):
        while self.killed is False:
            rpc = yield self.in_q.get()

            # Check for end of simulation.
            if self.check_req_end_sim(rpc):
                return

            rpcNumber = rpc.num
            rpc.start_proc_time = self.env.now

            # Model a service time
            if rpc.getWrite():
                yield self.env.timeout(self.write_distribution_generator.get())
            else:
                yield self.env.timeout(self.read_distribution_generator.get())

            # RPC is done
            rpc.end_proc_time = self.env.now
            rpc.completion_time = (
                self.env.now
            )  # This may need to be changed to model any "end of rpc" actions
            total_time = rpc.getTotalServiceTime()
            self.latency_store.record_value(total_time)
            self.putSTime(total_time)

            if self.isMaster is True and self.isSimulationUnstable() is True:
                print(
                    "Simulation was unstable, last five service times from core 0 were:",
                    self.lastFiveSTimes,
                    ", killing sim.",
                )
                self.endSimUnstable()
            self.numSimulated += 1

            if self.pull_queue is not None:
                self.pull_queue.put(PullFeedbackRequest(self.id, rpc))
            else:
                self.lb.func_executed(self.id, rpc.getFuncType())
