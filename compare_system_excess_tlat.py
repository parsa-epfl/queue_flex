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

## This experiment script generates the outputs for Figure 3 of the C-4 paper artifact.
## It compares CREW and d-CREW dispatch policies to an ideal one (single queue, no synch)
## in terms of excess tail latency, and outputs a CSV file with the results to be plotted.

import argparse
import simpy
import matplotlib as mpl
import numpy as np
import os
from datetime import datetime
from collections import OrderedDict

# Module interfaces
from parallel import Invoker
from components.mock_stime_gen import MockServiceTimeGenerator
from components.zipf_gen import ZipfKeyGenerator
from components.dispatch_policies.util import ascii_histogram
from components.load_range import RangeMaker
from util.csv_dict_ops import init_or_add_nested_dict, init_or_add_to_dic, write_csv
from util.iterator_helpers import cross_product

mpl.use("Agg")
# mpl.use("TkAgg")  # For macOS

import pandas as pd
from matplotlib import pyplot as plt

PERCENTILE = 99
SLO_BOUND = 10


def get_simpoints():
    SYNTH_WR_RANGE = np.arange(start=0, stop=105, step=5)
    return SYNTH_WR_RANGE


def parse_sim_results(rangeMaker, controller, output_fields, num_threads):
    results = [controller.getResultsFromQueue(idx) for idx in range(num_threads)]
    flat_results = [y for x in results for y in x]
    odict = {}
    for x in flat_results:
        for k, results_dict in x.items():
            norm_k = rangeMaker.get_max_normpt() / float(k)
            workload_percentiles = list(results_dict["percentiles_overall"])
            l = sorted(workload_percentiles, key=lambda t: t[0])
            l.append(("tput (MRPS)", results_dict["tput (MRPS)"]))
            l.append(("rd_99", results_dict["99th_perc_reads"]))
            for tup in l:
                if tup[0] not in output_fields:  # v[0] is the percentile (e.g., 95th)
                    output_fields.append(tup[0])
                init_or_add_nested_dict(odict, norm_k, tup[0], tup[1])
    return odict


def find_max_load_and_tlat(df, percentile, slo):
    # Sort dict by lowest load first and set max to lowest load
    sorted_d = OrderedDict(sorted(df.items(),key=lambda t: t[0]))
    max_load = list(df.keys())[0]
    tlat = df[max_load][percentile]

    # Search for higher load points
    for load, row in sorted_d.items(): # iterates from lowest->highest load b/c of sort
        if row[percentile] > slo:
            break
        elif load > max_load:
            max_load = load
            tlat = row[percentile]
    return max_load, tlat


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "zipf",
        help="Skew coefficient to fix for this experiment.",
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
        default=1.6 * 1e6,
    )
    parser.add_argument(
        "--hash_buckets",
        type=int,
        help="Number of hash buckets in the workload. Default = 1M",
        default=(1 << 20),
    )
    parser.add_argument(
        "--cores",
        type=int,
        help="Number of cores/RPC servers. Default  = 64",
        default=64,
    )
    parser.add_argument(
        "--channel_lat",
        type=int,
        default=30,
        help="Channel latency between dispatcher and cores. Default = 30ns.",
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
        help="Number of requests to simulate for. Default = 200k",
        default=200000,
    )
    parser.add_argument(
        "--odir",
        nargs="?",
        default="excess_tlat_comparison",
        help="Where to write the ouput file(s).",
    )
    parser.add_argument(
        "--turbo_boost",
        type=float,
        help="Turbo boost speed fraction to apply. All cores in CORES_TO_TURBO will get a service time reduction of (serv_time / turbo_boost). Default = 1.0 (Off)",
        default=1.0,
    )
    args = parser.parse_args()

    # Backup old directory if exists, and make new one
    if os.path.isdir(args.odir):
        backup_name = args.odir + "_bak_" + str(datetime.now())
        os.rename(args.odir, backup_name)
    os.mkdir(args.odir)

    # Create load range
    fake_env = simpy.Environment()
    stime_obj = MockServiceTimeGenerator(args.serv_time + args.fixed_overhead)
    rangeMaker = RangeMaker(
        stime_obj,
        args.cores,
        args.data_points,
        low_high_cutoff=0.5,
    )
    final_range = rangeMaker.make_load_range()

    csv_olist = []
    output_fields = ["load"]

    # Main loop
    kwarg_dict = {"num_items": args.num_items, "coeff": args.zipf}
    z = ZipfKeyGenerator(**kwarg_dict)
    for wr_frac in get_simpoints():
        print("*** Running experiments for write fraction = {}".format(wr_frac))
        # Setup optional arguments
        realistic_opt_args = {
            "ideal": False,
            "key_dist": z,
            "use_compaction": False,
        }

        num_indep_exps = max(int(int(args.threads) / 4),1)

        crew_invoker_args = {
            "numProcs": num_indep_exps,
            "runnableTarg": "mica_rlu_jbscrew",
            "mode": "sweep_A",
            "optargs": realistic_opt_args,
            "argrange": final_range,
            "serv_time": args.serv_time,
            "fixed_overhead": args.fixed_overhead,
            "disp_policy": "CREW",
            "write_frac": wr_frac,
            "channel_lat": args.channel_lat,
            "jbsq_depth": args.jbsq_depth,
            "reqs_to_sim": args.reqs_to_sim,
            "cores": args.cores,
            "hash_buckets": args.hash_buckets,
            "compaction_time": args.compaction_time,
            "turbo_boost": args.turbo_boost,
        }

        erew_invoker_args = {
            "numProcs": num_indep_exps,
            "runnableTarg": "mica_rlu_jbscrew",
            "mode": "sweep_A",
            "optargs": realistic_opt_args,
            "argrange": final_range,
            "serv_time": args.serv_time,
            "fixed_overhead": args.fixed_overhead,
            "disp_policy": "EREW",
            "write_frac": wr_frac,
            "channel_lat": args.channel_lat,
            "jbsq_depth": args.jbsq_depth,
            "reqs_to_sim": args.reqs_to_sim,
            "cores": args.cores,
            "hash_buckets": args.hash_buckets,
            "compaction_time": args.compaction_time,
            "turbo_boost": args.turbo_boost,
        }

        dcrew_invoker_args = {
            "numProcs": num_indep_exps,
            "runnableTarg": "mica_rlu_jbscrew",
            "mode": "sweep_A",
            "optargs": realistic_opt_args,
            "argrange": final_range,
            "serv_time": args.serv_time,
            "fixed_overhead": args.fixed_overhead,
            "disp_policy": "d-CREW",
            "write_frac": wr_frac,
            "channel_lat": args.channel_lat,
            "jbsq_depth": args.jbsq_depth,
            "reqs_to_sim": args.reqs_to_sim,
            "cores": args.cores,
            "hash_buckets": args.hash_buckets,
            "compaction_time": args.compaction_time,
            "turbo_boost": args.turbo_boost,
        }

        ideal_args = {
            "ideal": True,
            "key_dist": z,
            "use_compaction": False,
        }

        ideal_invoker_args = {
            "numProcs": num_indep_exps,
            "runnableTarg": "mica_rlu_jbscrew",
            "mode": "sweep_A",
            "optargs": ideal_args,
            "argrange": final_range,
            "serv_time": args.serv_time,
            "fixed_overhead": args.fixed_overhead,
            "disp_policy": "Ideal",
            "write_frac": 0,
            "channel_lat": args.channel_lat,
            "jbsq_depth": args.jbsq_depth,
            "reqs_to_sim": args.reqs_to_sim,
            "cores": args.cores,
            "hash_buckets": args.hash_buckets,
            "compaction_time": args.compaction_time,
        }

        ideal_controller = Invoker(**ideal_invoker_args)
        crew_controller = Invoker(**crew_invoker_args)
        dcrew_controller = Invoker(**dcrew_invoker_args)
        erew_controller = Invoker(**erew_invoker_args)

        print("*** Master thread starting parallel jobs....")
        # Start and join simulations
        ideal_controller.startProcs()
        crew_controller.startProcs()
        dcrew_controller.startProcs()
        erew_controller.startProcs()

        erew_controller.joinProcs()
        dcrew_controller.joinProcs()
        ideal_controller.joinProcs()
        crew_controller.joinProcs()

        # print("Realistic results for zipf {} and wr {}".format(zipf,wr_frac),odict_realistic)
        odict_crew = parse_sim_results(
            rangeMaker,
            crew_controller,
            output_fields,
            num_indep_exps,
        )
        crew_max_load, crew_max_tlat = find_max_load_and_tlat(
            odict_crew,
            PERCENTILE,
            SLO_BOUND * (args.serv_time + args.fixed_overhead),
        )
        # print("Real max load {}, real max tlat {}".format(real_max_load, real_max_tlat))

        odict_dcrew = parse_sim_results(
            rangeMaker,
            dcrew_controller,
            output_fields,
            num_indep_exps,
        )
        dcrew_max_load, dcrew_max_tlat = find_max_load_and_tlat(
            odict_dcrew,
            PERCENTILE,
            SLO_BOUND * (args.serv_time + args.fixed_overhead),
        )

        odict_erew = parse_sim_results(
            rangeMaker,
            erew_controller,
            output_fields,
            num_indep_exps,
        )
        erew_max_load, erew_max_tlat = find_max_load_and_tlat(
            odict_erew,
            PERCENTILE,
            SLO_BOUND * (args.serv_time + args.fixed_overhead),
        )

        odict_ideal = parse_sim_results(
            rangeMaker, ideal_controller, output_fields, num_indep_exps
        )
        ideal_max_load, ideal_max_tlat = find_max_load_and_tlat(
            odict_ideal, PERCENTILE, SLO_BOUND * (args.serv_time + args.fixed_overhead)
        )

        # print("For zipf {}, wr {}, results were".format(zipf,wr_frac),odict_realistic,odict_ideal)
        # print("Ideal max load {}, ideal max tlat {}".format(ideal_max_load, ideal_max_tlat))
        crew_slowdown = float(crew_max_load) / float(ideal_max_load)
        dcrew_slowdown = float(dcrew_max_load) / float(ideal_max_load)
        erew_slowdown = float(erew_max_load) / float(ideal_max_load)

        # Excess tlat is the percentile of the realistic, over the percentile of ideal, at the same load
        # Calculate for all designs.
        ctail_crew = odict_ideal[crew_max_load][PERCENTILE]
        ctail_dcrew = odict_ideal[dcrew_max_load][PERCENTILE]
        ctail_erew = odict_ideal[erew_max_load][PERCENTILE]
        excess_tlat_crew = float(crew_max_tlat) / float(ctail_crew)
        excess_tlat_dcrew = float(dcrew_max_tlat) / float(ctail_dcrew)
        excess_tlat_erew = float(erew_max_tlat) / float(ctail_erew)

        data_ = [
            wr_frac,
            ideal_max_load,
            ideal_max_tlat,
            crew_max_load,
            crew_max_tlat,
            excess_tlat_crew,
            dcrew_max_load,
            dcrew_max_tlat,
            excess_tlat_dcrew,
            erew_max_load,
            erew_max_tlat,
            excess_tlat_erew,
        ]
        csv_olist.append(data_)

        ofile_suffix = "excess_tlat_comparison.csv"
        fname = args.odir + "/" + ofile_suffix
        print("*** Writing CSV file {}".format(fname))
        output_fields = [
            "wr_frac",
            "ideal_max_load",
            "ideal_max_tlat",
            "crew_max_load",
            "crew_max_tlat",
            "crew_excess_tlat",
            "dcrew_max_load",
            "dcrew_max_tlat",
            "dcrew_excess_tlat",
            "erew_max_load",
            "erew_max_tlat",
            "erew_excess_tlat",
        ]
        write_csv(fname, output_fields, csv_olist)


if __name__ == "__main__":
    main()
