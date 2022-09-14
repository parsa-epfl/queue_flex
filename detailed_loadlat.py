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
import argparse
from numpy import linspace, concatenate, arange
from random import shuffle
from os.path import isfile
from shutil import copyfile
import csv
from functools import reduce

# Module interfaces
from parallel import Invoker
from components.mock_stime_gen import MockServiceTimeGenerator
from components.zipf_gen import ZipfKeyGenerator
from components.dispatch_policies.util import ascii_histogram
from components.load_range import RangeMaker
from components.fb_etc_lgen import ETCLoadGen
from multiprocessing import cpu_count
from exps.mica_rlu_jbscrew import get_statistics_keys

import simpy
import matplotlib as mpl
import os

# mpl.use("TkAgg")  # For macOS
if os.environ.get("DISPLAY", "") == "":
    # print('no display found. Using non-interactive Agg backend')
    mpl.use("Agg")
import pandas as pd
from matplotlib import pyplot as plt


def ecdf(dat):
    """Compute ecdf of data and return an x,y tuple to plot"""
    x_data = sorted(dat)
    n = len(x_data)
    y = arange(1, n + 1) / n
    return (x_data, y)


def plot_compacted_write_graphs(
    all_measurements, comp_write_measurements, read_dict, upper_slo
):
    """For each load point, make an output file which plots the distribution of the compacted writes, and then
    draws lines for the 90th, 99th, and 99.9th percentiles of the TOTAL distribution on it.
    """

    mpl.rc("font", **{"size": 9, "family": "serif"})
    for load, latency_store in comp_write_measurements.items():
        global_percentiles = sorted(all_measurements[load], key=lambda t: t[0])
        read_latencies = read_dict[load]

        fname = "comp_write_lat_load{:0.2}.pdf".format(load)
        fh = plt.figure(figsize=(4, 2))
        ax = fh.subplots()

        x, y = ecdf(latency_store.get_iterable())
        (
            x_r,
            y_r,
        ) = ecdf(read_latencies)

        ax.scatter(x, y, label="Comp. Writes", c="xkcd:grey", s=4, marker="^")
        ax.scatter(x_r, y_r, label="Reads", c="xkcd:ocean blue", s=4, marker="o")
        # Plot a line at each of the values in global_percentiles
        for p, val in global_percentiles:
            if p == 99:
                ax.vlines(
                    x=val,
                    ymin=0,
                    ymax=1,
                    colors="black",
                    ls=":",
                    lw=1,
                    label="p{}".format(p),
                )

        ax.vlines(
            x=upper_slo, ymin=0, ymax=1, colors="xkcd:red", ls="-", lw=1, label="SLO"
        )

        ticks_x = mpl.ticker.FuncFormatter(lambda x, pos: "{0:g}".format(x / 1e3))
        ax.xaxis.set_major_formatter(ticks_x)
        ax.grid(True, axis="y", linestyle="--", alpha=0.2, linewidth=0.5)
        ax.set_ylabel("ECDF")
        ax.set_xlabel(r"Latency $(\mu s)$")
        ax.set_ylim((0, 1))
        ax.set_xlim((0, upper_slo + 500))
        ax.legend(loc="upper right", framealpha=1)

        fh.tight_layout()
        fh.savefig(fname)
        print("Saved compacted write plot", fname)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        help="Dispatch policy to use. Default = CREW",
        choices=["EREW", "CREW", "CRCW", "d-CREW", "Ideal"],
    )
    parser.add_argument(
        "--starting_load",
        type=float,
        help="Normalized load to start the experiment from. Default = 0.05",
        default=0.05,
    )
    parser.add_argument(
        "--threads",
        type=int,
        help="Number of parallel threads to use. Default = 1",
        default=1,
    )
    parser.add_argument(
        "--serv_time",
        type=int,
        help="Total RPC Service time to model (ns). Default = 600",
        default=600,
    )
    parser.add_argument(
        "--fixed_overhead",
        type=int,
        help="Fixed overhead on each RPC (independent of distribution). Default = 100",
        default=100,
    )
    parser.add_argument(
        "--compaction_time",
        type=int,
        help="Time taken when a write is compacted, instead of the full service time. Default = 100",
        default=100,
    )
    parser.add_argument(
        "-N",
        "--num_items",
        type=int,
        help="Number of items in the dataset. Default = 1.6M",
        default=1.6 * 1e6
    )
    parser.add_argument(
        "--hash_buckets",
        type=int,
        help="Number of hash buckets in the workload. Default = 1M",
        default=(1 << 20)
    )
    parser.add_argument(
        "-s",
        "--zipf_coeff",
        type=float,
        help="Skew (zipf) coefficient. Default = 0.99",
        default=0.99,
    )
    parser.add_argument(
        "--cores",
        type=int,
        help="Number of cores/RPC servers. Default  = 64",
        default=64,
    )
    parser.add_argument(
        "--write_frac",
        type=float,
        help="Fraction of requests that are writes. Default = 25",
        default=25.0,
    )
    parser.add_argument(
        "--channel_lat",
        type=int,
        default=30,
        help="Channel latency between dispatcher and cores. Default = 30",
    )
    parser.add_argument(
        "--jbsq_depth",
        type=int,
        help="Upper limit on core-private depth for JBSQ(D) policy. Default = 2",
        default=2,
    )
    parser.add_argument(
        "--data_points",
        type=int,
        help="Number of data points between load range [0.05,1.0]. Default = 16",
        default=16,
    )
    parser.add_argument(
        "--reqs_to_sim",
        type=int,
        help="Number of requests to simulate for. Default = 1K",
        default=1000,
    )
    parser.add_argument(
        "--ofile",
        nargs="?",
        default="mica_dispatch.csv",
        help="The ouput file to write.",
    )
    parser.add_argument(
        "--use_etc",
        default=False,
        action="store_true",
        help="Use a load generator with size distributions coming from FB's ETC workload",
    )
    parser.add_argument(
        "--use_exp",
        default=False,
        action="store_true",
        help="Use exponentially distributed service times.",
    )
    parser.add_argument(
        "--use_bimod",
        default=False,
        action="store_true",
        help="Use bimodally distributed service times.",
    )
    parser.add_argument(
        "--use_compaction",
        default=False,
        action="store_true",
        help="Enable write compaction.",
    )
    parser.add_argument(
        "--plot_graphs",
        default=False,
        action="store_true",
        help="Plot a graph per load point showing the CDF of the compacted writes, compared to reads.",
    )
    parser.add_argument(
        "--turbo_boost",
        type=float,
        help="Turbo boost speed fraction to apply. All cores in CORES_TO_TURBO will get a service time reduction of (serv_time / turbo_boost). Default = 1.0 (Off)",
        default=1.0,
    )
    args = parser.parse_args()

    # Create load range
    fake_env = simpy.Environment()
    fake_store = simpy.Store(fake_env)
    stime_obj = MockServiceTimeGenerator(args.serv_time + args.fixed_overhead)
    rangeMaker = RangeMaker(
        stime_obj,
        args.cores,
        args.data_points,
        low_high_cutoff=0.5,
        starting_load=args.starting_load,
    )
    final_range = rangeMaker.make_load_range()

    # Make zipf distribution and key generator objects
    kwarg_dict = {"num_items": args.num_items, "coeff": args.zipf_coeff}
    z = ZipfKeyGenerator(**kwarg_dict)
    opt_args = {
        "key_dist": z,
        "use_etc": args.use_etc,
        "use_compaction": args.use_compaction,
        "use_exp": args.use_exp,
        "use_bimod": args.use_bimod,
    }

    invokerArgs = {
        "numProcs": int(args.threads),
        "runnableTarg": "mica_rlu_jbscrew",
        "mode": "sweep_A",
        "optargs": opt_args,
        "argrange": final_range,
        "serv_time": args.serv_time,
        "fixed_overhead": args.fixed_overhead,
        "disp_policy": args.mode,
        "write_frac": args.write_frac,
        "channel_lat": args.channel_lat,
        "jbsq_depth": args.jbsq_depth,
        "reqs_to_sim": args.reqs_to_sim,
        "cores": args.cores,
        "hash_buckets": args.hash_buckets,
        "compaction_time": args.compaction_time,
        "turbo_boost": args.turbo_boost,
    }

    print("*** Master thread starting parallel jobs....")
    threadController = Invoker(**invokerArgs)
    threadController.startProcs()
    threadController.joinProcs()

    results = [
        threadController.getResultsFromQueue(idx) for idx in range(int(args.threads))
    ]
    flat_results = [y for x in results for y in x]
    odict = {}
    output_fields = ["lambda"]

    output_boxplot_data = {}
    all_req_data = {}
    core_loc_data = {}
    rem_loc_data = {}
    write_bal_fractions = {}
    qp_count_histograms = {}
    the_99th_reqs = {}
    write_q_series = {}
    write_total_series = {}
    read_proc_series = {}
    read_total_series = {}
    spin_event_percentiles = abort_event_percentiles = {}
    bucket_hists = {}
    read_cc_percentages = {}
    num_compacted_writes = {}
    avg_write_time = {}

    def init_or_add(d, k, v):
        if k in d:
            d[k].append(v)
        else:
            d[k] = [v]

    statistics_keys = (
        get_statistics_keys()
    )  # must be defined in actual experiment file (e.g., in exps/mica_rlu_jbscrew.py)

    all_measurements = {}
    compacted_write_measurements = {}

    for x in flat_results:
        for k, results_dict in x.items():
            norm_k = rangeMaker.get_max_normpt() / float(k)
            total_q_counter_list = results_dict["simul_reqs_histograms"]
            indep_q_counter_list = results_dict["all_reqs_histograms"]
            total_counter = reduce(lambda x, y: x + y, total_q_counter_list)
            indep_counter = reduce(lambda x, y: x + y, indep_q_counter_list)
            output_boxplot_data[norm_k] = list(total_counter.elements())
            all_req_data[norm_k] = list(indep_counter.elements())
            core_loc_data[norm_k] = results_dict["cache_locality_fraction"]
            rem_loc_data[norm_k] = results_dict["remote_locality_fraction"]
            write_bal_fractions[norm_k] = results_dict["balanced_vs_excl_writes"]
            qp_count_histograms[norm_k] = results_dict["num_readers_to_wait"]
            the_99th_reqs[norm_k] = results_dict["the_99th_req"]
            read_proc_series[norm_k] = results_dict["read_proc_series"]
            read_total_series[norm_k] = results_dict["read_total_series"]
            write_q_series[norm_k] = results_dict["write_q_series"]
            write_total_series[norm_k] = results_dict["write_total_series"]
            spin_event_percentiles[norm_k] = results_dict["spin_event_percentiles"]
            abort_event_percentiles[norm_k] = results_dict["abort_event_percentiles"]
            bucket_hists[norm_k] = results_dict["bucket_load"]
            read_cc_percentages[norm_k] = results_dict["read_cc_percentage"]
            num_compacted_writes[norm_k] = results_dict["num_compacted_writes"]
            avg_write_time[norm_k] = results_dict["avg_write_time"]

            tail_perc_breakdown = results_dict["99th_perc_rw"]
            unpacked_percentiles = list(results_dict["percentiles_overall"])
            workload_percentiles = unpacked_percentiles
            all_measurements[norm_k] = unpacked_percentiles
            compacted_write_measurements[norm_k] = results_dict["final_delayed_hist"]

            # l = sorted(list(tail_perc_breakdown), key=lambda t: t[0])
            l = sorted(workload_percentiles, key=lambda t: t[0])
            l.append(("tput (MRPS)", results_dict["tput (MRPS)"]))
            l.append(("rd_99", results_dict["99th_perc_reads"]))
            for tup in l:
                if tup[0] not in output_fields:  # v[0] is the percentile (e.g., 95th)
                    output_fields.append(tup[0])
                init_or_add(odict, norm_k, tup[1])

    if args.plot_graphs:
        plot_compacted_write_graphs(
            all_measurements,
            compacted_write_measurements,
            read_dict=read_total_series,
            upper_slo=(args.serv_time + args.fixed_overhead) * 10,
        )

    """
    print("*** Percentiles of number of concurrency control spin events. ***")
    print(spin_event_percentiles)
    print("*** Percentiles of number of concurrency control abort events. ***")
    print(abort_event_percentiles)
    # Print compaction stats
    print("*** Percentiles of reads encountering CC. ***")
    print(read_cc_percentages)
    print("*** Number of compacted writes. ***")
    print(num_compacted_writes)
    print("*** Avg write service time. ***")
    print(avg_write_time)
    """
    """
    # Print histograms from counter
    perc_load_dict = {}
    for l, hist in bucket_hists.items():
        perc_load_dict[l] = {}
        total_reqs = sum(hist.values())
        for bucket, num_reqs in hist.items():
            perc_load_dict[l][bucket] = float(num_reqs) / float(total_reqs)
    print("*** Histograms of hash bucket load. ***")
    for l, percs in perc_load_dict.items():
        print("*** At lambda:",l," the buckets had loads:")
        for bucket, percentage in reversed(sorted(percs.items(),key=lambda item: item[1])):
            print("\tBucket {} = {}".format(bucket,percentage))
    for l, hist in bucket_hists.items():
        print("*** Lambda:",l,"has histogram:")
        ascii_histogram(hist,scale=1000)
    """

    """
    print("*** Histograms of number of readers that need to be waited for to indicate a quiescent period.")
    for load, val in qp_count_histograms.items():
        print("------ Load: {} ------ ".format(load))
        ascii_histogram(val,scale=50)
    """
    """
    loads = write_total_series.keys()
    mpl.rc("font", **{"size": 9, "family": "serif"})
    for norm_load in loads:
        # Print read proc/total CDFs
        fname = "tmp_graphs/read_cdf_lambda{}.pdf".format(norm_load)
        fh = plt.figure(figsize=(4, 2))
        ax = fh.subplots()
        val_series = read_proc_series[norm_load]
        val_series.hist(
            density=True,
            cumulative=True,
            ax=ax,
            bins=50,
            grid=True,
            label="Read Proc Time",
        )
        val_series = read_total_series[norm_load]
        val_series.hist(
            density=True,
            cumulative=True,
            ax=ax,
            bins=50,
            grid=True,
            label="Read Total Time",
        )
        ax.set_xlim(0, 6300)
        ax.legend(loc="lower right")
        fh.tight_layout()
        fh.savefig(fname)
        print("Saved plot", fname)
        plt.close(fh)

        # Print write q/total CDFs
        fname = "tmp_graphs/write_cdf_lambda{}.pdf".format(norm_load)
        fh = plt.figure(figsize=(4, 2))
        ax = fh.subplots()
        val_series = write_q_series[norm_load]
        val_series.hist(
            density=True,
            cumulative=True,
            ax=ax,
            bins=50,
            grid=True,
            label="Write Q Time",
        )
        val_series = write_total_series[norm_load]
        val_series.hist(
            density=True,
            cumulative=True,
            ax=ax,
            bins=50,
            grid=True,
            label="Write Total Time",
        )
        ax.set_xlim(0, 6300)
        ax.legend(loc="lower right")
        fh.tight_layout()
        fh.savefig(fname)
        print("Saved plot", fname)
        plt.close(fh)
    """

    # Print a bunch of histograms for write/read cdfs
    # print("*** Core Loc Data:", core_loc_data)
    # print("*** Remote Loc Data:", rem_loc_data)
    # print("*** Write Balancing Data:", write_bal_fractions)
    # print("*** 99th% Request Breakdown ***")
    # for load, req in the_99th_reqs.items():
    # print("@ Load {}, the 99th% req was: {}".format(load, req))

    fstring = args.ofile
    if isfile(fstring):
        print(fstring, "already present! Backing up to...", fstring + ".bak")
        copyfile(fstring, fstring + ".bak")

    with open(fstring, "w") as fh:
        writer = csv.DictWriter(fh, fieldnames=output_fields)
        writer.writeheader()
        for k, v in sorted(odict.items()):
            v.insert(0, k)
            tmp = dict(zip(output_fields, v))
            writer.writerow(tmp)


if __name__ == "__main__":
    main()
