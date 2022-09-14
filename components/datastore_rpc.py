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
from .rpc_core import AbstractCore
from .cache_state import PrivateDataCache, KVPair
from .cache_snoop_interface import CacheSnoopInterface
from .comm_channel import CommChannel
from .latency_store import LatencyStoreWithBreakdown, ExactLatStore
from .load_generator import AbstractLoadGen
from .bucketed_index import BucketedIndex, is_odd, AsyncIndexUpdater
from .load_balancer import LoadBalancer
from .end_measure import EndOfMeasurements
from .global_sequencer import GlobalSequencer
from .epoch_tracker import EpochTracker
from .deferral_controller import DeferralController
from .requests import PullFeedbackRequest
from .request_filter_lambdas import req_is_read, younger_read_to_different_bucket
from .serv_times.exp_generator import ExpServTimeGenerator
from .serv_times.fixed_generator import FixedServiceTime
from .serv_times.uniform_generator import UniformServTimeGenerator
from .serv_times.bimodal_stime import BimodalServTimeGenerator
from collections import Counter
from hdrh.histogram import HdrHistogram

import simpy
import typing
import copy

INDEX_UPDATE_DELAY = 50
BATCH_WINDOW_FACTOR = 10


def init_or_append(d, k, v):
    if k in d:
        d[k].append(v)
    else:
        d[k] = [v]


def key_matcher(req, k):
    if req.key == k:
        return True
    return False


class MICAIndexAccessor(AbstractCore, CacheSnoopInterface):
    """
    Models a RPC process which looks like a MICA RPC, involving an index for concurrency
    control and then a deterministic service time.
    """

    def __init__(
        self,
        simpy_env: simpy.Environment,
        core_id: int,
        serv_time: float,
        request_queue: CommChannel,
        measurements: LatencyStoreWithBreakdown,
        lgen_to_interrupt: AbstractLoadGen,
        load_balancer: LoadBalancer,
        bindex: BucketedIndex,
        pull_queue: CommChannel = None,
        disp_policy: str = "CREW",
        collect_queued_read_stats: bool = False,
        model_cache_locality: bool = True,
        cache_size: int = 65536,
        is_parent: bool = False,
        use_exp: bool = False,
        use_bimod: bool = False,
        use_compaction: bool = False,
        disregard_conf: bool = False,
        compaction_time=1,
        fixed_overhead: int = 1,
        turbo_boost: float = 1.0,
    ) -> None:
        super().__init__(core_id, lgen_to_interrupt)
        self.env = simpy_env
        self.id = core_id
        self.in_q = request_queue
        self.measurements = measurements
        self.pull_queue = pull_queue
        self.serv_time = serv_time
        self.fixed_overhead = fixed_overhead
        self.compaction_time = compaction_time
        self.use_compaction = use_compaction
        if use_exp:
            self.serv_time_generator = ExpServTimeGenerator(self.serv_time)
            self.overhead_generator = FixedServiceTime(self.fixed_overhead)
        elif use_bimod:
            ### These are hardcoded to be the values selected in our work. Should update to be configurable.
            short_time = self.serv_time / 2.0
            long_time = self.serv_time * 5.5
            perc_short = 90.0
            self.serv_time_generator = BimodalServTimeGenerator(
                perc_short, short_time, long_time
            )
            self.overhead_generator = FixedServiceTime(self.fixed_overhead)
        else:
            # self.serv_time_generator = FixedServiceTime(self.serv_time)
            self.serv_time /= turbo_boost
            self.fixed_overhead /= turbo_boost
            if self.serv_time < 200:
                self.serv_time_generator = UniformServTimeGenerator(
                    self.serv_time - 100, self.serv_time + 100
                )
                self.overhead_generator = FixedServiceTime(self.fixed_overhead)
            else:
                self.serv_time_generator = UniformServTimeGenerator(
                    self.serv_time - 200, self.serv_time + 200
                )
                self.overhead_generator = FixedServiceTime(self.fixed_overhead)

        self.bindex = bindex
        self.disp_policy = disp_policy
        self.killed = False
        self.numSimulated = 0
        self.collect_queued_read_stats = collect_queued_read_stats
        self.load_balancer = load_balancer
        self.total_q_histogram = Counter()
        self.matching_q_histogram = Counter()
        self.disregard_conf = disregard_conf

        # Vars and data for collecting cache locality
        self.gather_cache_locality = model_cache_locality
        self.cache_size = cache_size
        self.cache = PrivateDataCache(cache_size)
        self.cache_accesses = 0
        self.accesses_w_locality = 0
        self.accesses_w_remote_locality = 0
        self.rem_locality_histogram = Counter()

        # Vars and data for checking number of cc spins/aborts
        self.num_cc_spins = HdrHistogram(1, 100, 3)
        self.num_cc_aborts = HdrHistogram(1, 100, 3)
        self.reads_with_cc = 0
        self.reads = 0

        # Data for batching
        self.batch_map = {}
        self.batch_completion_time = {}
        self.batch_size_hist = Counter()
        self.delayed_write_latencies = ExactLatStore()

        # The actual running process
        self.reqs_completed = 0
        self.total_cycles_working = 0
        self.total_queued_time = 0

        if not is_parent:
            self.action = self.env.process(self.run())

    def collect_qstats_elsewhere(
        self,
        lfunc_to_match: typing.Callable,
    ) -> None:
        """
        Use interface to the load balancer to collect q stats from the other input
        queues. Stats are returned for all requests that match the lambda provided as
        this function's argument.
        """
        matching_req_dict = self.load_balancer.search_queues_for_matching_requests(
            [],  # Do not exclude any queues
            lfunc_to_match,
            include_processing_request=False,
        )
        all_reqs_dict = self.load_balancer.search_queues_for_matching_requests(
            [],
            req_is_read,
            include_processing_request=False,
        )
        total_reqs = 0
        num_matching = 0
        for k, v in all_reqs_dict.items():
            total_reqs += len(v)
        for k, v in matching_req_dict.items():
            num_matching += len(v)
        self.total_q_histogram[total_reqs] += 1
        self.matching_q_histogram[num_matching] += 1

    def req_completion_logic(self, rpc):
        rpc.end_proc_time = self.env.now
        yield self.env.timeout(self.overhead_generator.get())
        rpc.completion_time = (
            self.env.now
        )  # This may need to be changed to model any "end of rpc" actions

        # Record times
        total_time = rpc.getTotalServiceTime()
        self.measurements.record_value(rpc, total_time)
        self.putSTime(total_time)
        self.total_cycles_working += rpc.getProcessingTime()
        self.total_queued_time += rpc.getQueuedTime()
        # print("**** Worker {} finishing rpc {} at time {}".format(self.id,rpc,self.env.now))

        # If the request was a delayed write, record it specially in the delayed write queue.
        if rpc.getWrite() and rpc.was_delayed():
            self.delayed_write_latencies.record_value(total_time)

        # Check if the sim is unstable, die if so.
        if self.isMaster is True and self.isSimulationUnstable() is True:
            print(
                "Simulation was unstable, last five service times from core 0 were:",
                self.lastFiveSTimes,
                ", killing sim.",
            )
            self.endSimUnstable()
        self.numSimulated += 1
        if self.pull_queue is not None:
            # print("Worker {} putting pull request in queue at time {}".format(self.id,self.env.now))
            self.pull_queue.put(PullFeedbackRequest(self.id, rpc))
        else:
            self.lb.func_executed(self.id, rpc.getFuncType())

    def form_new_batch_logic(self, req_key) -> bool:
        for e in self.in_q.store.items:
            try:
                r = key_matcher(e, req_key)
                if r:
                    return True
            except AttributeError:
                continue
        return False

    def close_batch_logic(self, key) -> bool:
        comp_time = self.batch_completion_time[key]
        if self.env.now + 1.5 * self.serv_time >= comp_time:
            return True
        for e in self.in_q.store.items:
            try:
                r = key_matcher(e, key)
                if r:
                    return False
            except AttributeError:
                continue
        return True

    def respond_to_batched_writes(self, req_key):
        batched_list = self.batch_map[req_key]
        compacted_size = len(batched_list)
        self.batch_size_hist[compacted_size] += 1
        final_req = batched_list.pop()
        for req in batched_list:
            yield self.env.process(self.req_completion_logic(req))
            # print("Completed the batch completion logic for rpc",req,"at time",self.env.now)

        # Actually do write process for the final req
        bucket = hash(final_req) % self.bindex.get_num_buckets()
        # print("We started the batch write response at time",self.env.now)
        yield self.env.process(self.do_write_process(bucket))
        yield self.env.process(self.req_completion_logic(final_req))
        # print("Completed the batch write response and completion logic for rpc",final_req,"at time",self.env.now)
        del self.batch_map[req_key]
        del self.batch_completion_time[req_key]
        assert (
            req_key not in self.batch_map
        ), "Key {} was still in batch map at core {}".format(req_key, self.id)
        assert (
            req_key not in self.batch_completion_time
        ), "Key {} was still in batch completion time map at core {}".format(
            req_key, self.id
        )

    def check_batched_writes(self):
        remove_us = []
        for k, time in self.batch_completion_time.items():
            if self.env.now >= time:
                remove_us.append(k)
                yield self.env.process(self.respond_to_batched_writes(k))

        for k in remove_us:
            del self.batch_map[k]
            del self.batch_completion_time[k]
            assert (
                k not in self.batch_map
            ), "Key {} was still in batch map at core {}".format(k, self.id)
            assert (
                k not in self.batch_completion_time
            ), "Key {} was still in batch completion time map at core {}".format(
                k, self.id
            )

    def do_write_process(self, bucket):
        # print("Writer",self.id,"locking index",index,"at time",self.env.now)
        # self.bindex.inc_index_version(index)
        delayed_updater = AsyncIndexUpdater(
            self.env, self.bindex, bucket, INDEX_UPDATE_DELAY
        )
        yield self.env.timeout(self.serv_time_generator.get())
        assert (
            delayed_updater.finished is True
        ), "Writer {} finished a service time but async index update was not done!"
        # print("Writer",self.id,"unlocking index",index,"at time",self.env.now)
        delayed_updater = AsyncIndexUpdater(
            self.env, self.bindex, bucket, INDEX_UPDATE_DELAY
        )
        # self.bindex.inc_index_version(index)

    def run(self):
        while not self.killed:
            # yield self.env.process(self.check_batched_writes())
            start_new_req_get = self.env.now
            rpc = yield self.in_q.get()
            end_new_req_get = self.env.now
            if self.check_req_end_sim(rpc):
                continue
            blocking_time = end_new_req_get - start_new_req_get
            # if blocking_time > 10:
            # print("Core",self.id,"blocked waiting for a request for",blocking_time)
            rpcNumber = rpc.num
            rpc.start_proc_time = self.env.now
            num_hash_buckets = self.bindex.get_num_buckets()
            index = hash(rpc) % num_hash_buckets
            req_key = rpc.key
            # Single-param wrapper around req_to_different_bucket with the current hash
            # index/bucket count
            def check_req_not_matching(req):
                return younger_read_to_different_bucket(
                    req, index, rpcNumber, num_hash_buckets
                )

            batched_write = False
            if self.disp_policy == "EREW":
                # print("Core",self.id,"beginning req ID {} (write? = {}) at time {}. Hash = {}, bucket = {}".format(rpc.getID(),rpc.getWrite(),self.env.now,hash(rpc),index))
                # No version overhead required, pure dataplane
                assert is_odd(self.bindex.get_index_version(index)) is False
                if rpc.getWrite():
                    # Access the index, and increment its value before/after write completion
                    # Technically this is not mandatory, but we do this for sanity checking that no concurrency is happening.
                    # print("Core",self.id,"write req ID {} incrementing index {} at time {}".format(rpc.getID(),index,self.env.now))
                    self.bindex.inc_index_version(index)
                    yield self.env.timeout(self.serv_time_generator.get())
                    self.bindex.inc_index_version(index)
                    # print("Core",self.id,"END: write req ID {} incrementing index {} at time {}".format(rpc.getID(),index,self.env.now))
                else:
                    yield self.env.timeout(self.serv_time_generator.get())
            else:
                if self.disregard_conf:
                    yield self.env.timeout(self.serv_time_generator.get())
                else:
                    # Check index value and yield event (simulate spinning) until it's even
                    first_version = self.bindex.get_index_version(index)
                    while is_odd(first_version):
                        if not rpc.getWrite() and self.collect_queued_read_stats:
                            self.collect_qstats_elsewhere(check_req_not_matching)
                        rpc.num_cc_spins += 1
                        # print("Core",self.id,"yielding and spinning on index",index,"at time",self.env.now)
                        yield self.bindex.get_event_for_increment(self.env, index)
                        # print("Core",self.id,"waking at time",self.env.now)
                        first_version = self.bindex.get_index_version(index)

                    assert is_odd(self.bindex.get_index_version(index)) is False

                    if rpc.getWrite():
                        # Access the index, and increment its value before/after write completion
                        if self.use_compaction:
                            if req_key in self.batch_map:
                                # print("Already had batch started for",req_key,"fast-forwarding it at time",self.env.now)
                                batched_write = True
                                rpc.set_delayed(True)
                                init_or_append(self.batch_map, req_key, rpc)
                                yield self.env.timeout(self.compaction_time)
                                if self.close_batch_logic(req_key):
                                    # print("Closing batch for",req_key,"at time",self.env.now)
                                    yield self.env.process(
                                        self.respond_to_batched_writes(req_key)
                                    )
                            else:
                                if self.form_new_batch_logic(req_key):
                                    batched_write = True
                                    init_or_append(self.batch_map, req_key, rpc)
                                    rpc.set_delayed(True)
                                    self.batch_completion_time[req_key] = (
                                        self.env.now
                                        + BATCH_WINDOW_FACTOR * self.serv_time
                                    )
                                    # print("Starting NEW batch for",req_key,"at time",self.env.now,"final completion time limit:",self.batch_completion_time[req_key])
                                    yield self.env.timeout(self.compaction_time)
                                else:
                                    # print("Doing normal write")
                                    yield self.env.process(self.do_write_process(index))
                        else:
                            yield self.env.process(self.do_write_process(index))
                        # Wakeup waiting readers (REMOVED USING AsyncIndexUpdater)
                        # self.bindex.succeed_event_for_bucket(index)
                        # print("Writer",self.id,"waking up index",index,"at time",self.env.now)
                    else:  # READ
                        # if self.use_compaction and req_key in self.batch_map:
                        # yield self.env.timeout(self.compaction_time)
                        prev_version = self.bindex.get_index_version(index)
                        yield self.env.timeout(self.serv_time_generator.get())
                        # Optimistic reads have to re-check versions to confirm lack of intervening writers
                        recheck = self.bindex.get_index_version(index)
                        if prev_version != recheck:
                            # print("Reader",self.id,"retrying on index",index,"at time",self.env.now)
                            rpc.num_cc_aborts += 1
                            yield self.env.timeout(self.serv_time_generator.get())

            # Model cache statistics if instructed to do so
            if self.gather_cache_locality:
                kvpair = KVPair(
                    key=hash(rpc),
                    value="unimportant",
                    key_size=rpc.get_key_size(),
                    value_size=rpc.get_val_size(),
                )
                # print("Core",self.id,"looking up key",kvpair.key())
                hit, evicted_list = self.cache.access(kvpair)
                # print("Result:",hit,evicted_list)
                self.cache_accesses += 1
                if hit:
                    self.accesses_w_locality += 1
                else:
                    core_list_without_self = self.remote_cores
                    del core_list_without_self[self.id]
                    rc_hits = list(
                        map(lambda x: x.snoop_cache(kvpair), core_list_without_self)
                    )
                    if any(rc_hits):
                        self.accesses_w_remote_locality += 1
                    num_rc_hits = sum(rc_hits)
                    self.rem_locality_histogram[num_rc_hits] += 1

            # RPC is done
            if not (batched_write):
                yield self.env.process(self.req_completion_logic(rpc))

            # Record number of cc hits/aborts
            if not rpc.getWrite():
                self.reads += 1
                self.num_cc_spins.record_value(rpc.num_cc_spins)
                self.num_cc_aborts.record_value(rpc.num_cc_aborts)
                if rpc.num_cc_aborts != 0 or rpc.num_cc_spins != 0:
                    self.reads_with_cc += 1

    def snoop_cache(self, pair: KVPair) -> bool:
        """Overrides CacheSnoopInterface.snoop_cache(...) for this private cache."""
        return self.cache.peek(pair)

    def set_remote_cores(self, remote_cores: typing.List[AbstractCore]):
        """Give this core a list of all other cores/caches in the simulation."""
        self.remote_cores = copy.copy(remote_cores)

    def get_batched_hist(self) -> Counter:
        """Return this core's histogram of batched size"""
        return self.batch_size_hist


class MultiversionMICAIndexAccessor(MICAIndexAccessor):
    def __init__(
        self,
        simpy_env: simpy.Environment,
        core_id: int,
        serv_time: float,
        request_queue: CommChannel,
        measurements: LatencyStoreWithBreakdown,
        lgen_to_interrupt: AbstractLoadGen,
        load_balancer: LoadBalancer,
        bindex: BucketedIndex,
        global_sequencer: GlobalSequencer,
        epoch_tracker: EpochTracker,
        wr_deferral_controller: DeferralController,
        pull_queue: CommChannel = None,
        disp_policy: str = "CREW",
        collect_queued_read_stats: bool = False,
        model_cache_locality: bool = True,
        cache_size: int = 65536,
        use_exp: bool = False,
        write_defer: bool = False,
    ) -> None:
        super().__init__(
            simpy_env,
            core_id,
            serv_time,
            request_queue,
            measurements,
            lgen_to_interrupt,
            load_balancer,
            bindex,
            pull_queue,
            disp_policy,
            collect_queued_read_stats,
            model_cache_locality,
            cache_size,
            use_exp=use_exp,
            is_parent=True,
        )
        self.global_sequencer = global_sequencer
        self.epoch_tracker = epoch_tracker

        self.action = self.env.process(self.run())

        # state variables for RLU
        self.write_ts = None
        self.quiescent_period_intervals = ExactLatStore()
        self.num_readers_to_wait = Counter()
        self.defer_writes = write_defer
        self.wr_deferral_controller = wr_deferral_controller

    def get_writer_ts(self) -> int:
        """Return the local writer timestamp."""
        return self.write_ts

    def rlu_synchronize(self, cur_epoch, index) -> None:
        """Model an rlu_synchronize() event."""
        # print("Writer",core_id,"set global ts to",self.write_ts)
        self.write_ts = self.global_sequencer.increment_ts()
        # RLU paper says cur_epoch++? why?
        num_readers = self.epoch_tracker.num_readers_registered(cur_epoch)
        # self.num_readers_to_wait[num_readers] += 1
        (
            readers_ended_event,
            ts_at_sync,
        ) = self.epoch_tracker.writer_synchronize_epoch(cur_epoch)
        yield readers_ended_event
        ts_at_wakeup = self.env.now
        blocked_time = ts_at_wakeup - ts_at_sync

        if self.defer_writes:
            # Model extra cost that scales with the number of deferred writes prior
            cost_mutiplier = self.wr_deferral_controller.get_deferral_cost_multiplier()
            yield self.env.timeout(
                self.serv_time_generator.get_with_mean(
                    cost_mutiplier * self.serv_time * 0.1
                )
            )
        else:
            # Model simple extra cost for reader/writer communication
            extra_cost = num_readers * 30
            if blocked_time < extra_cost:
                yield self.env.timeout(
                    self.serv_time_generator.get_with_mean(extra_cost - blocked_time)
                )

        # print("Writer",core_id,"waited a quiescent period, epoch", cur_epoch,"is over")
        self.bindex.set_index_version(index, 0)  # unlock
        self.wr_deferral_controller.reset_defer()

    def run(self):
        """Override the parent run() to model a multiversioned index accessor, where reads/writes have proper concurrency."""
        core_id = self.id

        while not self.killed:
            rpc = yield self.in_q.get()
            if self.check_req_end_sim(rpc):
                continue
            # Book-keeping to start a request
            rpc.start_proc_time = self.env.now
            num_hash_buckets = self.bindex.get_num_buckets()
            index = hash(rpc) % num_hash_buckets

            if rpc.getWrite():  # write path
                assert self.disp_policy == "CREW" or self.disp_policy == "d-CREW"
                cur_epoch = self.epoch_tracker.get_cur_epoch()
                # print("Writer",core_id,"starting with epoch",cur_epoch)

                # Check index, if there was a previous writer that deferred without unlocking, this thread needs
                # to synchronize and then it can write itself.
                locked_core_id = self.bindex.get_index_version(index) - 1
                if locked_core_id > 0:
                    self.rlu_synchronize(cur_epoch, index)

                # lock index by putting my id there, signal to readers
                self.bindex.set_index_version(
                    index, core_id + 1
                )  # 0 is special, unlocked
                # print("Writer",core_id,"locked bucket",index)

                # fixed cost for log and update all objects in datastore
                yield self.env.timeout(
                    self.serv_time_generator.get_with_mean(self.serv_time * 1.15)
                )

                # End of processing time
                rpc.end_proc_time = self.env.now

                if self.defer_writes:
                    if self.wr_deferral_controller.check_defer():
                        self.rlu_synchronize(cur_epoch, index)
                    else:
                        # print("Writer",core_id,"deferred its writes, defer counter shows",self.write_defer_counter,"writes remaining until rlu_synchronize.")
                        yield self.env.timeout(
                            self.serv_time_generator.get_with_mean(self.serv_time * 0.1)
                        )  # cleanup cost
                else:
                    self.rlu_synchronize(cur_epoch, index)

                # End of synchronize or deferred cleanup time.
                rpc.completion_time = self.env.now
                # self.quiescent_period_intervals.record_value(rpc.getPostProcessingTime())
                self.write_ts = None
                # print("Writer",core_id,"completed processing")

            else:  # read
                cur_epoch = self.epoch_tracker.register_reader(core_id)
                # print("Reader", core_id, "registered itself on epoch",cur_epoch)
                locked_core_id = self.bindex.get_index_version(index) - 1
                if locked_core_id > 0:  # intervening writer
                    remote_wr_ts = self.remote_cores[locked_core_id].get_writer_ts()
                    if remote_wr_ts is None:
                        yield self.env.timeout(
                            self.serv_time_generator.get_with_mean(
                                self.serv_time * 1.03
                            )
                        )
                    else:
                        if cur_epoch >= remote_wr_ts:  # steal
                            yield self.env.timeout(
                                self.serv_time_generator.get_with_mean(
                                    self.serv_time * 1.1
                                )
                            )
                        else:  # read old copy
                            yield self.env.timeout(
                                self.serv_time_generator.get_with_mean(
                                    self.serv_time * 1.03
                                )
                            )
                else:
                    yield self.env.timeout(
                        self.serv_time_generator.get_with_mean(self.serv_time * 1.03)
                    )
                rpc.end_proc_time = self.env.now

                # print("Reader", core_id, "unregistering itself on epoch",cur_epoch)
                self.epoch_tracker.unregister_reader(
                    epoch_number=cur_epoch, reader_id=core_id
                )
                rpc.completion_time = self.env.now

            # Record times
            total_time = rpc.getTotalServiceTime()
            self.measurements.record_value(rpc, total_time)
            self.putSTime(total_time)

            if self.isMaster is True and self.isSimulationUnstable() is True:
                print(
                    "Simulation was unstable, last five service times from core 0 were:",
                    self.lastFiveSTimes,
                    ", killing sim.",
                )
                self.endSimUnstable()
            self.numSimulated += 1
            # print("Core {} finished req id {} with service time {}.".format(self.id,rpc.getID(),total_time))

            if self.pull_queue is not None:
                self.pull_queue.put(PullFeedbackRequest(self.id, rpc))
            else:
                self.lb.func_executed(self.id, rpc.getFuncType())
