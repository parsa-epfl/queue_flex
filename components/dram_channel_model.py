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
from simpy import Resource
from math import floor
from random import randint
from math import floor, ceil
from .requests import NetworkedRPCRequest
from .comm_channel import CommChannel

# some random DRAM parameters, can un-hardcode this later
tCAS = 14
tRP = 14
tRAS = 24
tOffchip = 25
RB_HIT_RATE = 75


class SyncOverlappedMemoryRequest(object):
    def __init__(self, env, resource_queues, sz, completionSignal, interRequestTime=1):
        self.env = env
        self.queues = resource_queues
        self.size = sz
        self.interRequestTime = interRequestTime
        self.completionSignal = completionSignal
        self.action = env.process(self.run())

    def run(self):
        num_reqs = floor(self.size / 64)
        eventArray = [self.env.event() for i in range(num_reqs)]
        for i in range(num_reqs):
            SingleMemoryRequest(self.env, self.queues, eventArray[i])
            if i < (num_reqs - 1):
                yield self.env.timeout(self.interRequestTime)

        # sleep until all events are fulfilled
        for i in range(num_reqs):
            yield eventArray[i]

        self.completionSignal.succeed()


# Also a separate process to run independently to the above requester
class AsyncMemoryRequest(object):
    def __init__(self, env, resource_queues, sz, interRequestTime=1):
        self.env = env
        self.queues = resource_queues
        self.size = sz
        self.interRequestTime = interRequestTime
        self.action = env.process(self.run())

    def run(self):
        num_reqs = ceil(self.size / 64)
        eventArray = [self.env.event() for i in range(num_reqs)]
        for i in range(num_reqs):
            # print('created new single req. at',self.env.now)
            SingleMemoryRequest(self.env, self.queues, eventArray[i])
            if i < (num_reqs - 1):
                yield self.env.timeout(self.interRequestTime)

        # sleep until all events are fulfilled
        for i in range(num_reqs):
            # print('asyncmemreqest yielding/passivating at',self.env.now)
            yield eventArray[i]
            # print('asyncmemreqest activating at',self.env.now)


class BWBucket(object):
    def __init__(self, start, end):
        self.start_time = start
        self.end_time = end
        self.bytesTransferred = 0

    def addReq(self, sz):
        self.bytesTransferred += sz

    def asTuple(self):
        return (self.start_time, self.end_time, self.bytesTransferred)

    def getIntervalBW(self):
        return self.bytesTransferred / float(self.end_time - self.start_time)  # GB/s


class BWProfiler(object):
    def __init__(self, e, nbanks, bucketInterval):
        self.env = e
        self.nbanks = nbanks
        self.interval = bucketInterval
        self.buckets = []
        self.currentBucket = BWBucket(self.env.now, self.env.now + self.interval)

    def completeReq(self, t, sz):
        if t > self.currentBucket.end_time:
            self.buckets.append(self.currentBucket)  # finished this one, make new
            nextStartTime = self.currentBucket.end_time
            self.currentBucket = BWBucket(nextStartTime, nextStartTime + self.interval)
            # print('making new BW bucket with range [',nextStartTime,',',nextStartTime + self.interval,']')
        # print('completing DRAM req of size',sz,'at time',t)
        self.currentBucket.addReq(sz)

    def getBucketBWs(self):
        self.buckets.append(self.currentBucket)  # terminate current bucket
        return [i.getIntervalBW() for i in self.buckets]


class InfiniteQueueDRAM(Resource):
    def __init__(self, env, nbanks):
        super().__init__(env, nbanks)
        self.num_banks = nbanks
        self.env = env

        INTERVAL = 10000  # 10 us
        self.profiler = BWProfiler(env, nbanks, INTERVAL)

    def getIntervalBandwidths(self):
        return self.profiler.getBucketBWs()

    def getBankLatency(self):
        r = randint(0, 100)
        if r <= RB_HIT_RATE:
            return tOffchip + tCAS
        else:
            return tOffchip + tRP + tRAS + tCAS

    def completeReq(self, sz):
        self.profiler.completeReq(self.env.now, sz)


class SingleMemoryRequest(object):
    def __init__(self, env, resource_queues, eventToSucceed):
        self.env = env
        self.queues = resource_queues
        self.event = eventToSucceed
        self.action = env.process(self.run())

    # runs and then fulfills the event when it's done
    def run(self):
        # print('Single request starting at',self.env.now)
        q = self.queues[randint(0, len(self.queues) - 1)]
        with q.request() as req:
            yield req
            yield self.env.timeout(q.getBankLatency())
        q.completeReq(64)
        # raise the event
        # print('Single request raising event at',self.env.now)
        self.event.succeed()


# Dispatch policy that runs independently to its requestor
class RPCDispatchRequest(object):
    def __init__(
        self,
        env,
        resource_queues,
        sz,
        eventCompletion,
        interRequestTime,
        dispatch_q,
        rnum,
        rpc_q_dat_array,
        collect_qdat,
        load_balancer_obj,
        no_dispatch=False,
        q_idx=0,
    ):
        self.env = env
        self.queues = resource_queues
        self.size = sz
        self.interRequestTime = interRequestTime
        self.eventCompletion = eventCompletion
        self.dispatch_queue = dispatch_q
        self.num = rnum
        self.q_idx = q_idx
        self.no_dispatch = no_dispatch
        self.rpc_q_dat_array = rpc_q_dat_array
        self.collect_qdat = collect_qdat
        self.load_balancer_object = load_balancer_obj
        self.action = self.env.process(self.run())

    def run(self):
        num_reqs = ceil(float(self.size) / 64)
        eventArray = [self.env.event() for i in range(num_reqs)]
        for i in range(num_reqs):
            # print('RPC',self.num,'created new single req. at',self.env.now)
            SingleMemoryRequest(self.env, self.queues, eventArray[i])
            if i < (num_reqs - 1):
                yield self.env.timeout(self.interRequestTime)

        # call to upper layer
        # print('RPC',self.num,'completed packet writes at',self.env.now)
        self.eventCompletion.succeed()

        if self.no_dispatch is False:
            newRPC = NetworkedRPCRequest(
                self.num, self.env.now, False
            )  # ddio miss on writing payloads to dram
            # sleep until all events are fulfilled
            for i in range(num_reqs):
                yield eventArray[i]
                # print('RPC',self.num,'has packet write event completed at time',self.env.now)
            if self.collect_qdat is True:
                if isinstance(self.dispatch_queue, CommChannel):
                    shared_queue_depth = self.dispatch_queue.num_items_enqueued()
                else:
                    shared_queue_depth = len(self.dispatch_queue.items)
                total_num_queued = (
                    shared_queue_depth
                    + self.load_balancer_object.num_reqs_in_private_qs()
                )
                self.rpc_q_dat_array.append((self.num, self.q_idx, total_num_queued))
            self.dispatch_queue.put(newRPC)
