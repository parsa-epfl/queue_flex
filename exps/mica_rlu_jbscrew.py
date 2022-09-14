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

# Models a system with N cores accessing a shared hash index protected by some form of RLU
# with a JBSCREW dispatch policy.

from components.load_balancer import DynamicEWLoadBalancer, LoadBalancer
from components.load_generator import OpenPoissonLoadGen
from components.dispatch_policies.util import ascii_histogram
from components.dispatch_policies.key_based_policies import (
    CREWDispatchPolicy,
    EREWDispatchPolicy,
    CRCWDispatchPolicy,
)
from components.dispatch_policies.JBSQ import (
    JBSQDispatchPolicy,
    JBSCREWDispatchPolicy,
    DynJBSCREWDispatchPolicy,
)
from components.comm_channel import CommChannel
from components.bucketed_index import BucketedIndex
from components.fb_etc_lgen import ETCLoadGen
from components.epoch_tracker import EpochTracker
from components.global_sequencer import GlobalSequencer
from components.latency_store import LatencyStoreWithBreakdown
from components.datastore_rpc import MICAIndexAccessor, MultiversionMICAIndexAccessor
from components.deferral_controller import DeferralController

from hdrh.histogram import HdrHistogram
from collections import Counter
import argparse
import functools
import simpy
import random
import pandas as pd

from util.csv_dict_ops import get_from_dic_or_false

# Used by the higher level invoking script to parse which statistics are returned by this experiment.
def get_statistics_keys():
    return [
        "99th_perc_rw",
        "percentiles_overall",
        "simul_reqs_histograms",
        "all_reqs_histograms",
        "cache_locality_fraction",
        "remote_locality_fraction",
        "balanced_vs_excl_writes",
        "num_readers_to_wait",
        "tput (MRPS)",
        "99th_perc_reads",
    ]


CORES_TO_TURBO = [29, 46, 58]
# CORES_TO_TURBO = [58]
# CORES_TO_TURBO = []


def run_exp_w_optargs(arg_string, optional_arg_objects):
    parser = argparse.ArgumentParser(
        description="Basic simulation to compare tail latency in MICA-RLU."
    )
    parser.add_argument(
        "--disp_policy",
        help="Dispatch policy to use. Default = CREW",
        choices=["EREW", "CREW", "CRCW", "d-CREW", "Ideal"],
    )
    parser.add_argument(
        "-c",
        "--cores",
        type=int,
        default=16,
        help="Number of worker cores in the queueing system. Default = 16",
    )
    parser.add_argument(
        "-a",
        "--arrival_rate",
        type=float,
        help="RPC inter-arrival time for the load gen. Default = 1",
        default=1.0,
    )
    parser.add_argument(
        "--reqs_to_sim",
        type=int,
        help="Number of requests to simulate for. Default = 1K",
        default=1000,
    )
    parser.add_argument(
        "--hash_buckets",
        type=int,
        help="Number of hash buckets in the workload.",
        default=1000000,
    )
    parser.add_argument(
        "--jbsq_depth",
        type=int,
        help="Upper limit on core-private depth for JBSQ(D) policy. Default = 2",
        default=2,
    )
    parser.add_argument(
        "-s",
        "--serv_time",
        dest="serv_time",
        type=int,
        default=1000,
        help="Service time of the RPC",
    )
    parser.add_argument(
        "--fixed_overhead",
        dest="fixed_overhead",
        type=int,
        default=300,
    )
    parser.add_argument(
        "--channel_lat",
        type=int,
        default=30,
        help="Channel latency between dispatcher and cores.",
    )
    parser.add_argument(
        "--defer_count",
        type=int,
        default=1,
        help="How many writes to defer before executing a synchronization event.",
    )
    parser.add_argument(
        "--write_frac",
        type=float,
        help="Fraction of requests that are writes",
        default=5.0,
    )
    parser.add_argument(
        "--compaction_time",
        type=int,
        help="Time taken when a write is compacted, instead of the full service time. Default = 200",
        default=200,
    )
    parser.add_argument(
        "--turbo_boost",
        type=float,
        help="Turbo boost speed fraction to apply. All cores in TURBO_BOOST_LIST (in this file) will get a service time reduction of (serv_time / turbo_boost). Default = 1.0 (Off)",
        default=1.0,
    )
    args = parser.parse_args(arg_string.split(" "))
    zdist = optional_arg_objects["key_dist"]
    use_etc = get_from_dic_or_false(optional_arg_objects, "use_etc")
    use_mvcc = get_from_dic_or_false(optional_arg_objects, "use_mvcc")
    use_exp = get_from_dic_or_false(optional_arg_objects, "use_exp")
    use_bimod = get_from_dic_or_false(optional_arg_objects, "use_bimod")
    use_compaction = get_from_dic_or_false(optional_arg_objects, "use_compaction")

    random.seed(0xcafed00d) # seed default random number generator

    # measurements = HdrHistogram(1, 100000, 3)
    measurements = LatencyStoreWithBreakdown(store_objects=False)
    env = simpy.Environment()

    ## Create event queue between the NI and LB
    event_queue = CommChannel(env, delay=1)

    # Dispatch queues (one per core) with a finite latency
    disp_queues = [CommChannel(env, delay=args.channel_lat) for i in range(args.cores)]

    # Shared pull queue where cores report to the LB that their requests are done
    pull_queue = CommChannel(env, delay=args.channel_lat)

    disregard_conf = False
    if args.disp_policy == "CREW":
        # disp_policy = CREWDispatchPolicy(args.cores, disp_queues)
        disp_policy = JBSCREWDispatchPolicy(
            env, disp_queues, args.jbsq_depth, args.hash_buckets
        )
    elif args.disp_policy == "d-CREW":
        disp_policy = DynJBSCREWDispatchPolicy(
            env,
            disp_queues,
            args.jbsq_depth,
            num_hash_buckets=args.hash_buckets,
            max_buckets_tracking=args.cores * args.jbsq_depth,
        )
    elif args.disp_policy == "EREW":
        disp_policy = EREWDispatchPolicy(args.cores, disp_queues, args.hash_buckets)
    elif args.disp_policy == "Ideal":
        disp_policy = JBSQDispatchPolicy(env, disp_queues, args.jbsq_depth)
        disregard_conf = True
    else:
        disp_policy = CRCWDispatchPolicy(args.cores, disp_queues)

    # Create load generator and load balancer
    if use_etc:
        assert True == False, "Config tried to create ETCLoadGen, not yet supported!"
        lg = ETCLoadGen(
            simpy_env=env,
            out_queue=event_queue,
            num_events=args.reqs_to_sim,
            key_obj=zdist,
            incoming_load=args.arrival_rate,
            writes=args.write_frac,
        )
    else:
        lg = OpenPoissonLoadGen(
            env,
            event_queue,
            int(args.reqs_to_sim),
            zdist,
            args.arrival_rate,
            args.write_frac,
        )

    # Create hash index
    hash_index = BucketedIndex(args.hash_buckets)

    if args.disp_policy == "d-CREW":
        lb = DynamicEWLoadBalancer(
            env, lg, event_queue, disp_queues, hash_index, pull_queue, disp_policy
        )
    else:
        lb = LoadBalancer(env, lg, event_queue, disp_queues, pull_queue, disp_policy)
    # lg.set_lb(lb)

    # Create global sequencer and epoch tracker
    gs = GlobalSequencer(initial_value=1)
    et = EpochTracker(gs, env)

    def_ctrl = DeferralController(args.defer_count)

    # Create cores and hook up to dispatch and pull queues.
    do_cache_loc_exp = False
    collect_queued_reads = False
    if use_mvcc:
        cores = [
            MultiversionMICAIndexAccessor(
                env,
                i,
                args.serv_time,
                disp_queues[i],
                measurements,
                lg,
                lb,
                hash_index,
                gs,
                et,
                def_ctrl,
                pull_queue,
                args.disp_policy,
                collect_queued_read_stats=collect_queued_reads,
                model_cache_locality=do_cache_loc_exp,
                cache_size=64 * 1024,
                use_exp=use_exp,
                write_defer=True,
                fixed_overhead=args.fixed_overhead,
                compaction_time=args.compaction_time,
                use_compaction=use_compaction,
                disregard_conf=disregard_conf,
                turbo_boost=args.turbo_boost if i in CORES_TO_TURBO else 1.0,
            )
            for i in range(args.cores)
        ]
    else:
        cores = [
            MICAIndexAccessor(
                env,
                i,
                args.serv_time,
                disp_queues[i],
                measurements,
                lg,
                lb,
                hash_index,
                pull_queue,
                args.disp_policy,
                collect_queued_read_stats=collect_queued_reads,
                model_cache_locality=do_cache_loc_exp,
                cache_size=64 * 1024,
                use_exp=use_exp,
                use_bimod=use_bimod,
                use_compaction=use_compaction,
                fixed_overhead=args.fixed_overhead,
                compaction_time=args.compaction_time,
                disregard_conf=disregard_conf,
                turbo_boost=args.turbo_boost if i in CORES_TO_TURBO else 1.0,
            )
            for i in range(args.cores)
        ]

    for c in cores:
        c.set_remote_cores(cores)

    #print("*** Experiment with lambda", args.arrival_rate, "starting! ***")

    # Kickoff simulation
    # Run for time interval which is equivalent to requests * (serv_time + overhead)
    # simulation_time = args.reqs_to_sim * (args.serv_time + args.fixed_overhead) / float(args.cores)
    env.run()
    est_throughput = float(measurements.get_total_count()) / (env.now * 1e-9) / 1e6
    """
    print(
        "Finished sim for lambda",
        args.arrival_rate,
        "recorded",
        measurements.get_total_count(),
        "measurements, in",
        env.now,
        "units of time. Total throughput = {} MRPS (est)".format(est_throughput),
    )
    """

    # Aggregate histograms of all cores' number of QP readers.
    aggregate = Counter()
    if use_mvcc:
        for c in cores:
            aggregate.update(c.num_readers_to_wait)

    # Print histograms of queue depth just for the LULZ
    """
    for i in range(len(cores)):
        print("*** Queue depth histogram for core #{}:".format(i))
        ascii_histogram(lb.histograms_for_core(i),scale=5000)
        print("*** Core",i,"completed",cores[i].reqs_completed,"requests")
    """

    # Print quiescent period stats
    """
    percentiles = [ 50, 95, 99, 99.9 ]
    for c in cores:
        print("*** Quiescent periods for core",c.id)
        for p in percentiles:
            print("\t{}th percentile = {}".format(p,c.quiescent_period_intervals.get_value_at_percentile(p)))
    """

    headers = [
        "wr_q_time",
        "wr_proc_time",
        "wr_sync_time",
        "rd_q_time",
        "rd_proc_time",
        "rd_sync_time",
        "tput (MRPS)",
    ]
    tail_req_wr = measurements.get_req_at_percentile(
        99, filter_reqs=True, is_write=True
    )
    if tail_req_wr is not None:
        w_qtime = tail_req_wr.getQueuedTime()
        w_ptime = tail_req_wr.getProcessingTime()
        w_synctime = tail_req_wr.getPostProcessingTime()
    else:
        w_qtime = w_ptime = w_synctime = 0
    tail_req_rd = measurements.get_req_at_percentile(
        99, filter_reqs=True, is_write=False
    )
    if tail_req_rd is not None:
        r_qtime = tail_req_rd.getQueuedTime()
        r_ptime = tail_req_rd.getProcessingTime()
        r_synctime = tail_req_rd.getPostProcessingTime()
    else:
        r_qtime = r_ptime = r_synctime = 0
    the_99th_req = measurements.get_req_at_percentile(99, filter_reqs=False)
    vals = [w_qtime, w_ptime, w_synctime, r_qtime, r_ptime, r_synctime, est_throughput]

    # Return pandas Series of all read/write objects
    read_objs = measurements.get_read_objects()
    write_objs = measurements.get_write_objects()
    if read_objs is not None and write_objs is not None:
        read_proc_series = pd.Series([x.getProcessingTime() for x in read_objs])
        read_total_series = pd.Series([x.getTotalServiceTime() for x in read_objs])
        write_q_series = pd.Series([x.getQueuedTime() for x in write_objs])
        write_total_series = pd.Series([x.getTotalServiceTime() for x in write_objs])
    else:
        read_proc_series = (
            read_total_series
        ) = write_q_series = write_total_series = None

    simul_reqs_histograms = [cores[i].matching_q_histogram for i in range(args.cores)]
    all_reqs_histograms = [cores[i].total_q_histogram for i in range(args.cores)]

    # Get overall percentiles for this simulation.
    percentiles = [50, 90, 99, 99.9]
    overall_percentile_values = [
        measurements.get_global_latency_percentile(p) for p in percentiles
    ]
    overall_read_tail = measurements.get_filtered_latency_percentile(99, reads=True)

    # Get cache locality
    if do_cache_loc_exp:
        cache_accesses = [cores[i].cache_accesses for i in range(args.cores)]
        cache_hits = [cores[i].accesses_w_locality for i in range(args.cores)]
        remote_locality = [
            cores[i].accesses_w_remote_locality for i in range(args.cores)
        ]
        cache_locality_fraction = [
            float(cache_hits[i]) / cache_accesses[i] for i in range(args.cores)
        ]
        remote_locality_fraction = [
            remote_locality[i] / (float(cache_accesses[i] - cache_hits[i]))
            for i in range(args.cores)
        ]
    else:
        cache_locality_fraction = []
        remote_locality_fraction = []

    # Make dictionary for number of cc spins/aborts at various percentiles
    final_spin_hist = cores[0].num_cc_spins
    final_abort_hist = cores[0].num_cc_aborts
    for i in range(1, args.cores):
        final_spin_hist.add(cores[i].num_cc_spins)
        final_abort_hist.add(cores[i].num_cc_aborts)

    spin_event_percentiles = final_spin_hist.get_percentile_to_value_dict(percentiles)
    abort_event_percentiles = final_abort_hist.get_percentile_to_value_dict(percentiles)

    # Get % of reads hitting CC, and compaction size
    reads_per_core = [cores[i].reads for i in range(args.cores)]
    writes_per_core = [
        cores[i].numSimulated - cores[i].reads for i in range(args.cores)
    ]
    reads_with_cc_per_core = [cores[i].reads_with_cc for i in range(args.cores)]
    compaction_histograms = [cores[i].get_batched_hist() for i in range(args.cores)]
    num_reads_total = functools.reduce(lambda x, y: x + y, reads_per_core, 0)
    num_writes_total = functools.reduce(lambda x, y: x + y, writes_per_core, 0)
    num_rds_cc_total = functools.reduce(lambda x, y: x + y, reads_with_cc_per_core, 0)
    hist_total = functools.reduce(lambda x, y: x + y, compaction_histograms)

    if num_reads_total != 0:
        read_cc_percentage = float(num_rds_cc_total) / float(num_reads_total)
    else:
        read_cc_percentage = 0.0

    # Calculate total write service time reduction due to compaction. For a particular compaction degree N,
    # the total latency is: (N-1)*(compaction time+fixed_overhead) + (serv_time+fixed_overhead)
    def calc_compaction_time(degree, repeat_count):
        time = repeat_count * (
            (degree - 1) * (args.compaction_time + args.fixed_overhead)
            + (args.serv_time + args.fixed_overhead)
        )
        # print("Degree {} and repeat count {} returning total time {}".format(degree,repeat_count,time))
        return time

    num_compacted_writes = functools.reduce(
        lambda x, y: x + (y[0] * y[1]), hist_total.most_common(), 0
    )  # total number is the product of the batching degree and repeat count, summed over all the histogram
    total_compacted_time = functools.reduce(
        lambda x, y: x + calc_compaction_time(y[0], y[1]), hist_total.most_common(), 0
    )
    if num_compacted_writes > 0:
        avg_compacted_write_time = float(total_compacted_time) / num_compacted_writes
    else:
        avg_compacted_write_time = 0
    if num_writes_total > 0:
        weighted_avg_time = (
            float(
                total_compacted_time
                + (
                    (num_writes_total - num_compacted_writes)
                    * (args.serv_time + args.fixed_overhead)
                )
            )
            / num_writes_total
        )
    else:
        weighted_avg_time = 0
    # print("Total num. compacted writes:",num_compacted_writes,"average compacted write time:",avg_compacted_write_time,"total avg write time:",weighted_avg_time)
    # print(
    # "Final compaction size histogram for lambda {}: {}".format(
    # args.arrival_rate, hist_total
    # )
    # )

    # Create a final histogram for all the delayed/compacted writes, and return it to the master thread.
    delayed_write_histograms = [
        cores[i].delayed_write_latencies for i in range(args.cores)
    ]
    final_delayed_hist = delayed_write_histograms.pop()
    for h in delayed_write_histograms:
        final_delayed_hist.add(h)

    # Create a similar histogram for the reads
    final_read_histo = measurements.get_read_objects()

    # Get data for fraction of balanced vs excl writes
    if isinstance(disp_policy, DynJBSCREWDispatchPolicy):
        dat = disp_policy.get_wr_statistics()
    else:
        dat = {}

    if isinstance(disp_policy, JBSCREWDispatchPolicy):
        bucket_load = disp_policy.bucket_load_counter
    else:
        bucket_load = {}

    working_cycles = [cores[i].total_cycles_working for i in range(args.cores)]
    queued_cycles = [cores[i].total_queued_time for i in range(args.cores)]
    sum_working = functools.reduce(lambda x, y: x + y, working_cycles)
    sum_queued = functools.reduce(lambda x, y: x + y, queued_cycles)

    # print("For load",args.arrival_rate,"cores worked for {} cycles in aggregate and requests were queued for {} in aggregate.".format(sum_working,sum_queued))

    return {
        "99th_perc_rw": zip(headers, vals),
        "percentiles_overall": zip(percentiles, overall_percentile_values),
        "simul_reqs_histograms": simul_reqs_histograms,
        "all_reqs_histograms": all_reqs_histograms,
        "cache_locality_fraction": cache_locality_fraction,
        "remote_locality_fraction": remote_locality_fraction,
        "balanced_vs_excl_writes": dat,
        "num_readers_to_wait": aggregate,
        "tput (MRPS)": est_throughput,
        "99th_perc_reads": overall_read_tail,
        "the_99th_req": the_99th_req,
        "read_proc_series": read_proc_series,
        "read_total_series": read_total_series,
        "write_q_series": write_q_series,
        "write_total_series": write_total_series,
        "spin_event_percentiles": spin_event_percentiles,
        "abort_event_percentiles": abort_event_percentiles,
        "bucket_load": bucket_load,
        "read_cc_percentage": read_cc_percentage,
        "avg_write_time": weighted_avg_time,
        "num_compacted_writes": num_compacted_writes,
        "bucket_load": bucket_load,
        "final_delayed_hist": final_delayed_hist,
    }
