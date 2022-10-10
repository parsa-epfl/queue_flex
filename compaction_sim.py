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
## Compares a CREW dispatch policy to an ideal one (single queue, no synch)
## across a range of skew and write combinations.

import argparse
import simpy
import matplotlib as mpl
import numpy as np
import os
from collections import OrderedDict

# Module interfaces
from parallel import Invoker
from components.mock_stime_gen import MockServiceTimeGenerator
from components.zipf_gen import ZipfKeyGenerator
from components.dispatch_policies.util import ascii_histogram
from components.load_range import RangeMaker
from util.csv_dict_ops import init_or_add_nested_dict, init_or_add_to_dic, write_csv
from util.iterator_helpers import cross_product

if os.environ.get("DISPLAY", "") == "":
    # print('no display found. Using non-interactive Agg backend')
    mpl.use("Agg")
else:
    mpl.use("TkAgg")  # For macOS

import pandas as pd
from matplotlib import pyplot as plt

PERCENTILE = 99
SLO_BOUND = 10


def get_simpoints():
    SYNTH_ZIPF_RANGE = np.arange(start=0.9, stop=1.5, step=0.1)
    SYNTH_WR_RANGE = np.arange(start=0, stop=85, step=5)
    #SYNTH_ZIPF_RANGE = np.arange(start=1.3, stop=1.6, step=0.1)
    #SYNTH_WR_RANGE = np.arange(start=0, stop=80, step=5)
    return cross_product(SYNTH_ZIPF_RANGE, SYNTH_WR_RANGE)


def parse_sim_results(rangeMaker, flat_results, odict, output_fields):
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
        "mode",
        help="Dispatch policy to use. Default = CREW",
        choices=["EREW", "CREW", "CRCW", "d-CREW"],
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
        help="Channel latency between dispatcher and cores.",
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
        help="Number of data points between load range [0.05,1.0]. Default = 24",
        default=24,
    )
    parser.add_argument(
        "--reqs_to_sim",
        type=int,
        help="Number of requests to simulate for. Default = 200k",
        default=200000,
    )
    parser.add_argument(
        "--ofile",
        nargs="?",
        default="crew_ideal_comparison.csv",
        help="The ouput file to write.",
    )
    parser.add_argument(
        "--use_compaction",
        default=False,
        action="store_true",
        help="Enable write compaction.",
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
    last_zipf = 0
    z = None
    for zipf, wr_frac in get_simpoints():
        print("*** Running experiments for (zipf, wr.) combination of ({},{})".format(zipf,wr_frac))
        num_indep_exps = max(int(int(args.threads) / 4),1)

        if zipf != last_zipf:  # Update the zipf to new coefficient, otherwise can skip
            kwarg_dict = {"num_items": args.num_items, "coeff": zipf}
            z = ZipfKeyGenerator(**kwarg_dict)
            last_zipf = zipf

        # Setup optional arguments
        opt_args = {
            "ideal": False,
            "key_dist": z,
            "use_compaction": args.use_compaction,
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
            "use_compaction": args.use_compaction,
        }

        ideal_invoker_args = {
            "numProcs": int(args.threads),
            "runnableTarg": "mica_rlu_jbscrew",
            "mode": "sweep_A",
            "optargs": ideal_args,
            "argrange": final_range,
            "serv_time": args.serv_time,
            "fixed_overhead": args.fixed_overhead,
            "disp_policy": "CRCW",
            "write_frac": 0,
            "channel_lat": args.channel_lat,
            "jbsq_depth": args.jbsq_depth,
            "reqs_to_sim": args.reqs_to_sim,
            "cores": args.cores,
            "hash_buckets": args.hash_buckets,
            "compaction_time": args.compaction_time,
        }

        ideal_controller = Invoker(**ideal_invoker_args)
        realistic_controller = Invoker(**invokerArgs)
        print("*** Master thread starting parallel jobs....")
        realistic_controller.startProcs()
        ideal_controller.startProcs()

        realistic_controller.joinProcs()
        ideal_controller.joinProcs()

        realistic_results = [
            realistic_controller.getResultsFromQueue(idx)
            for idx in range(int(args.threads))
        ]
        realistic_flat_results = [y for x in realistic_results for y in x]
        odict_realistic = {}
        parse_sim_results(
            rangeMaker, realistic_flat_results, odict_realistic, output_fields
        )
        # print("Realistic results for zipf {} and wr {}".format(zipf,wr_frac),odict_realistic)
        real_max_load, real_max_tlat = find_max_load_and_tlat(
            odict_realistic,
            PERCENTILE,
            SLO_BOUND * (args.serv_time + args.fixed_overhead),
        )
        # print("Real max load {}, real max tlat {}".format(real_max_load, real_max_tlat))

        ideal_results = [
            ideal_controller.getResultsFromQueue(idx)
            for idx in range(int(args.threads))
        ]
        ideal_flat_results = [y for x in ideal_results for y in x]
        odict_ideal = {}
        parse_sim_results(rangeMaker, ideal_flat_results, odict_ideal, output_fields)
        ideal_max_load, ideal_max_tlat = find_max_load_and_tlat(
            odict_ideal, PERCENTILE, SLO_BOUND * (args.serv_time + args.fixed_overhead)
        )

        # print("For zipf {}, wr {}, results were".format(zipf,wr_frac),odict_realistic,odict_ideal)
        # print("Ideal max load {}, ideal max tlat {}".format(ideal_max_load, ideal_max_tlat))
        realistic_slowdown = float(real_max_load) / float(ideal_max_load)

        # Excess tlat is the percentile of the realistic, over the percentile of ideal, at the same load
        corresponding_tail_ideal = odict_ideal[real_max_load][PERCENTILE]
        excess_tlat = float(real_max_tlat) / float(corresponding_tail_ideal)
        data_ = [
            zipf,
            wr_frac,
            ideal_max_load,
            ideal_max_tlat,
            real_max_load,
            real_max_tlat,
            realistic_slowdown,
            excess_tlat,
        ]
        csv_olist.append(data_)

        print("*** Writing CSV file {}".format(args.ofile))
        output_fields = [
            "zipf",
            "wr_frac",
            "ideal_max_load",
            "ideal_max_tlat",
            "real_max_load",
            "real_max_tlat",
            "tput_slowdown",
            "excess_tlat",
        ]
        write_csv(args.ofile, output_fields, csv_olist)


if __name__ == "__main__":
    main()
