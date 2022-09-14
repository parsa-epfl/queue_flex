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

# Models a system with N cores accessing a shared hash index with various dispatch policies
# (EREW,CREW,CRCW)

from components.load_balancer import LoadBalancer, IndexAwareLoadBalancer
from components.load_generator import OpenPoissonLoadGen
from components.rpc_core import MICAIndexAccessor
from components.dispatch_policies.util import ascii_histogram
from components.dispatch_policies.key_based_policies import (
    EREWDispatchPolicy,
    CREWDispatchPolicy,
    CRCWDispatchPolicy,
)
from components.dispatch_policies.JBSQ import JBSCREWDispatchPolicy
from components.comm_channel import CommChannel
from components.bucketed_index import BucketedIndex
from components.fb_etc_lgen import ETCLoadGen

import simpy

# python environment includes
import argparse
from hdrh.histogram import HdrHistogram
from components.latency_store import LatencyStoreWithBreakdown


def run_exp_w_optargs(arg_string, optional_arg_objects):
    parser = argparse.ArgumentParser(
        description="Basic simulation to compare tail latency in MICA dispatch policies."
    )
    parser.add_argument(
        "--disp_policy",
        help="Dispatch policy to use. Default = CREW",
        choices=["EREW", "CREW", "CRCW", "d-CREW"],
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
        "--channel_lat",
        type=int,
        default=30,
        help="Channel latency between dispatcher and cores.",
    )
    parser.add_argument(
        "--write_frac",
        type=float,
        help="Fraction of requests that are writes",
        default=5.0,
    )
    parser.add_argument(
        "--use_etc",
        type=bool,
        default=True,
        help="Use a load generator with size distributions coming from FB's ETC workload",
    )
    args = parser.parse_args(arg_string.split(" "))

    zdist = optional_arg_objects["key_dist"]

    # measurements = HdrHistogram(1, 100000, 3)
    measurements = LatencyStoreWithBreakdown()
    env = simpy.Environment()

    ## Create event queue between the NI and LB
    event_queue = CommChannel(env, delay=1)

    # Dispatch queues (one per core) with a finite latency
    disp_queues = [CommChannel(env, delay=args.channel_lat) for i in range(args.cores)]

    # Shared pull queue where cores report to the LB that their requests are done
    pull_queue = CommChannel(env, delay=args.channel_lat)

    if args.disp_policy == "CREW" or args.disp_policy == "d-CREW":
        # disp_policy = CREWDispatchPolicy(args.cores, disp_queues)
        disp_policy = JBSCREWDispatchPolicy(env, disp_queues, args.jbsq_depth)
    elif args.disp_policy == "EREW":
        disp_policy = EREWDispatchPolicy(args.cores, disp_queues)
    else:
        disp_policy = CRCWDispatchPolicy(args.cores, disp_queues)

    # Create load generator and load balancer
    if args.use_etc:
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
        lb = IndexAwareLoadBalancer(
            env, lg, event_queue, disp_queues, hash_index, pull_queue, disp_policy
        )
    else:
        lb = LoadBalancer(env, lg, event_queue, disp_queues, pull_queue, disp_policy)
    # lg.set_lb(lb)

    # Create cores and hook up to dispatch and pull queues.
    do_cache_loc_exp = False
    collect_queued_reads = True
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
        )
        for i in range(args.cores)
    ]

    for c in cores:
        c.set_remote_cores(cores)

    # Kickoff simulation
    env.run()
    est_throughput = float(measurements.get_total_count()) / (env.now * 1e-9) / 1e6
    print(
        "Finished sim for lambda",
        args.arrival_rate,
        "recorded",
        measurements.get_total_count(),
        "measurements, in",
        env.now,
        "units of time. Total throughput = {} MRPS (est)".format(est_throughput),
    )

    # Print histograms of queue depth just for the LULZ
    """
    for i in range(len(cores)):
        print("*** Histogram for core #{}:".format(i))
        ascii_histogram(lb.histograms_for_core(i),scale=5000)
    """

    # percentiles = [ 50, 95, 99, 99.9 ]
    headers = ["wr_q_time", "wr_proc_time", "rd_q_time", "rd_proc_time", "tput (MRPS)"]
    tail_req_wr = measurements.get_req_at_percentile(99, is_write=True)
    if tail_req_wr is not None:
        w_qtime = tail_req_wr.getQueuedTime()
        w_ptime = tail_req_wr.getProcessingTime()
    else:
        w_qtime = w_ptime = 0
    tail_req_rd = measurements.get_req_at_percentile(99, is_write=False)
    if tail_req_rd is not None:
        r_qtime = tail_req_rd.getQueuedTime()
        r_ptime = tail_req_rd.getProcessingTime()
    else:
        r_qtime = r_ptime = 0
    vals = [w_qtime, w_ptime, r_qtime, r_ptime, est_throughput]
    simul_reqs_histograms = [cores[i].matching_q_histogram for i in range(args.cores)]
    all_reqs_histograms = [cores[i].total_q_histogram for i in range(args.cores)]

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
    return [
        zip(headers, vals),
        simul_reqs_histograms,
        all_reqs_histograms,
        cache_locality_fraction,
        remote_locality_fraction,
    ]
