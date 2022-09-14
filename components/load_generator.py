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
from simpy import Environment, Interrupt, Store
from numpy.random import Generator, PCG64
from .end_measure import EndOfMeasurements
from .requests import RPCRequest, NetworkedRPCRequest
from .dram_channel_model import *
from .comm_channel import CommChannel

# Python base package includes
from random import randint
from tqdm import tqdm, trange


def rollHit(prob_success):
    """
    Return true or false with a given probability between [0.00,100.00].

    Takes in a float in range [0.0,100.0] representing probability of success (e.g., 99.3)
    rollHit supports decimals up to 2 hundredths of precision
    """
    p_scaled = int(prob_success * 100)
    r = randint(0, 10000)
    if r < p_scaled:
        return True
    return False


class AbstractLoadGen(object):
    def __init__(self):
        pass


class OpenPoissonLoadGen(AbstractLoadGen):
    """
    A class which serves as an open-loop Poisson load generator.
    """

    def __init__(
        self, simpy_env, out_queue, num_events, key_obj, incoming_load, writes
    ):
        super().__init__()
        self.env = simpy_env
        self.q = out_queue
        self.num_events = num_events
        self.myLambda = incoming_load
        self.key_generator = key_obj
        self.write_frac = writes
        self.action = self.env.process(self.run())

        self.rseed = 0xdeadbeef
        self.numpy_randgen = Generator(PCG64(self.rseed))

    def gen_new_req(self, rpc_id=-1):
        # Setup parameters like id, key, etc
        is_write = rollHit(self.write_frac)
        rank = self.key_generator.get_rank()
        req = RPCRequest(
            rpc_id,
            k=rank,
            write=is_write,
            predef_hash=self.key_generator.hash_for_rank(rank),
        )
        req.generated_time = self.env.now
        return req

    def run(self):
        numGenerated = 0
        for numGenerated in range(self.num_events):
            try:
                new_req = self.gen_new_req(numGenerated)
                self.q.put(new_req)
                yield self.env.timeout(self.numpy_randgen.exponential(self.myLambda))
            except Interrupt as i:
                print(
                    "LoadGenerator killed during event generation. Interrupt:",
                    i,
                    "die....",
                )
                return

        # Make a new EndOfMeasurements event (special)
        self.q.put(EndOfMeasurements())

        # Keep generating events for realistic measurement
        while True:
            try:
                self.q.put(self.gen_new_req(-1))
                yield self.env.timeout(self.numpy_randgen.exponential(self.myLambda))
            except Interrupt as i:
                return


## A class which serves as a open loop load generator but with the additional capability
## of modelling memory bandwidth.
class NILoadGen(AbstractLoadGen):
    def __init__(
        self,
        env,
        ArrivalRate,
        dram_queues,
        p_ddio,
        RPCSize,
        queue_to_lb,
        N,
        dataplanes,
        collect_qdat,
    ):
        super().__init__()
        self.env = env
        self.q = queue_to_lb
        self.reqs_per_rpc = ceil(float(RPCSize) / 64)
        self.myLambda = ArrivalRate / self.reqs_per_rpc
        self.prob_ddio = p_ddio
        self.RPCSize = RPCSize
        self.numRPCs = N
        self.dataplane_dispatch = dataplanes
        self.collect_qdat = collect_qdat
        self.dram_queues = dram_queues
        self.load_balancer_object = None

        # Data array containing tuples to be written to a queue depth CSV in the following format:
        #   <rpc num>,<q_num>,<q_depth>
        self.rpc_q_dat_array = []
        self.action = env.process(self.run())

        self.rseed = 0xdeadbeef
        self.numpy_randgen = Generator(PCG64(self.rseed))

    # Sort the q dat array by q_depth
    def get99th_queued(self):
        sorted_by_q_depth = sorted(self.rpc_q_dat_array, key=lambda tup: tup[2])
        idx_tail = floor(len(sorted_by_q_depth) * 0.99)
        # print('Sorted by q_depth',sorted_by_q_depth)
        return sorted_by_q_depth[idx_tail][2]

    def set_lb(self, the_lb):
        self.load_balancer_object = the_lb

    def run(self):
        numSimulated = 0
        while numSimulated < self.numRPCs:
            try:
                ddio_hit = rollHit(self.prob_ddio)
                if ddio_hit is True:
                    for i in range(self.reqs_per_rpc):
                        if i < (self.reqs_per_rpc - 1):
                            yield self.env.timeout(self.myLambda)
                    newRPC = NetworkedRPCRequest(numSimulated, self.env.now, ddio_hit)
                    if self.collect_qdat is True:
                        if isinstance(self.q, CommChannel):
                            shared_queue_depth = self.q.num_items_enqueued()
                        else:
                            shared_queue_depth = len(self.q.items)
                        total_num_queued = (
                            shared_queue_depth
                            + self.load_balancer_object.num_reqs_in_private_qs()
                        )
                        self.rpc_q_dat_array.append(
                            (numSimulated, 0, total_num_queued)
                        )  # use q_idx = 0 for single q
                    self.q.put(newRPC)
                else:
                    # Launch a multi-packet request to memory, dispatch when it is done.
                    # print("Starting new dispatch request at time",self.env.now)
                    payloadsDoneEvent = self.env.event()
                    payloadWrite = RPCDispatchRequest(
                        self.env,
                        self.dram_queues,
                        self.RPCSize,
                        payloadsDoneEvent,
                        self.myLambda,
                        self.q,
                        numSimulated,
                        self.rpc_q_dat_array,
                        self.collect_qdat,
                        self.load_balancer_object,
                    )
                    # Roll hit probability, and if fail, do a writeback
                    hit_clean = rollHit(self.prob_ddio)
                    if hit_clean is False:
                        AsyncMemoryRequest(self.env, self.dram_queues, self.RPCSize)
                    yield payloadsDoneEvent  # all payloads written

                yield self.env.timeout(self.numpy_randgen.exponential(self.myLambda))
                numSimulated += 1
            except Interrupt as i:
                print("NI killed with Simpy exception:", i, "....EoSim")
                return

        self.q.put(EndOfMeasurements())  # Only put 1 EndOfMeasurements() event.

        # After the dispatch is done, keep generating the traffic for realistic measurements.
        while True:
            try:
                ddio_hit = rollHit(self.prob_ddio)
                if ddio_hit is True:
                    for i in range(self.reqs_per_rpc):
                        if i < (self.reqs_per_rpc - 1):
                            yield self.env.timeout(self.myLambda)
                else:
                    # Launch a multi-packet request to memory, but don't dispatch it
                    payloadsDoneEvent = self.env.event()
                    payloadWrite = RPCDispatchRequest(
                        self.env,
                        self.dram_queues,
                        self.RPCSize,
                        payloadsDoneEvent,
                        self.myLambda,
                        self.q,
                        numSimulated,
                        self.rpc_q_dat_array,
                        self.collect_qdat,
                        self.load_balancer_object,
                        no_dispatch=True,
                    )
                    yield payloadsDoneEvent  # all payloads written

                yield self.env.timeout(self.numpy_randgen.exponential(self.myLambda))
            except Interrupt as i:
                # print("NI killed in post-dispatch phase, exception:",i,"....End of Sim...")
                return
