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

# Plot a comparison of "excess tail latency" between multiple systems.
# Data is generated from "compare_system_excess_tlat.py"

# Mark Sutherland
# (C) 2021

import argparse
import os
import re
import matplotlib as mpl
import numpy

mpl.use("Agg")
mpl.rc("font", **{"size": 9})
import pandas as pd
from matplotlib import pyplot as plt

mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42

data_to_colour_dict = {
    "EREW": "xkcd:dark red",
    "CREW": "xkcd:black",
    "Dynamic": "xkcd:prussian blue",
}

key_to_colour_dict = {
    "erew_excess_tlat": "xkcd:dark red",
    "crew_excess_tlat": "xkcd:black",
    "dcrew_excess_tlat": "xkcd:prussian blue",
}

key_to_legkey = {
    "erew_excess_tlat": "EREW",
    "crew_excess_tlat": "CREW",
    "dcrew_excess_tlat": "Dynamic",
}

key_to_name_load = {
    "erew_max_load": "EREW",
    "crew_max_load": "CREW",
    "dcrew_max_load": "Dynamic",
}

key_to_load_leg = {}

linestyles = ["-", "--", "-.", ":"]
markers = ["s", "o", "x", "v"]
m_sizes = [3, 3, 3, 3]
cur = 0

fig_width_cm = 4
golden_ratio = 1.618033
# fig_height_cm = fig_width_cm/golden_ratio
fig_height_cm = 1.65


def calc_normalized_loads(df):
    for k, data_name in key_to_name_load.items():
        normalized_series_name = data_name + "_norm_max"
        df[normalized_series_name] = df[k] / df["ideal_max_load"]
        key_to_load_leg[normalized_series_name] = data_name


def plot_achievable_load(args, df):
    cur = 0
    fname = args.output_file_achievable_load
    fh = plt.figure(figsize=(fig_width_cm, 1.2))
    ax = fh.subplots(1)

    for k, data_name in key_to_load_leg.items():
        leg_key = data_name
        col = data_to_colour_dict[data_name]
        df.plot(
            x="wr_frac",
            y=k,
            linestyle=linestyles[cur],
            label=leg_key,
            marker=markers[cur],
            markersize=m_sizes[cur],
            ax=ax,
            color=col,
        )
        cur += 1

    box = ax.get_position()
    ax.set_position(
        [
            box.x0 + box.width * 0.10,
            box.y0 + box.height * 0.12,
            box.width * 0.90,
            box.height * 0.8,
        ]
    )
    # ylim = 630 * 10
    ax.set_ylim(0.5, 1.05)
    # ax.set_yticks(numpy.arange(0,1,0.25))
    ax.set_yticks([0.5, 0.75, 1])
    ax.set_xlim(0, 100)
    # ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])

    ax.tick_params(axis="both", which="major", **{"top": False, "bottom": True})
    ax.set_xlabel("Write Fraction")
    ax.set_ylabel("Peak Tput.\n@ SLO")
    ax.legend(
        loc="upper left",
        bbox_to_anchor=(0.05, 1.4),
        ncol=3,
        columnspacing=1,
        labelspacing=1,
        handletextpad=0.1,
        frameon=False,
    )
    # ax.grid(True, axis="both", linestyle="--", alpha=0.25, linewidth=0.5)
    fh.savefig(fname, dpi=1000)
    print("Saved plot", fname)


def plot_excess_tlat(args, df):
    cur = 0
    fname = args.output_file_excess_tlat
    fh = plt.figure(figsize=(fig_width_cm, fig_height_cm))
    ax = fh.subplots(1)

    for k, col in key_to_colour_dict.items():
        leg_key = key_to_legkey[k]
        df.plot(
            x="wr_frac",
            y=k,
            linestyle=linestyles[cur],
            label=leg_key,
            marker=markers[cur],
            markersize=m_sizes[cur],
            ax=ax,
            color=col,
        )
        cur += 1

    box = ax.get_position()
    ax.set_position(
        [
            box.x0 + box.width * 0.10,
            box.y0 + box.height * 0.2,
            box.width * 0.90,
            box.height * 0.85,
            # box.x0 + box.width * 0.04,
            # box.y0 + box.height * 0.18,
            # box.width,
            # box.height * 0.775,
        ]
    )
    # ylim = 630 * 10
    # ax.set_ylim(0, ylim)
    ax.set_yticks([0, 1, 2, 3, 4, 5, 6])
    ax.set_xlim(0, 100)
    # ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])

    ax.tick_params(axis="both", which="major", **{"top": False, "bottom": True})
    ax.set_xlabel("Write Fraction")
    ax.set_ylabel("99th% Norm.\nto Ideal")
    """
    ax.legend(
        loc="upper left",
        bbox_to_anchor=(-0.1, 1.28),
        ncol=3,
        columnspacing=1,
        labelspacing=1,
        handletextpad=0.1,
        frameon=False,
    )
    """
    ax.get_legend().remove()
    # ax.grid(True, axis="both", linestyle="--", alpha=0.25, linewidth=0.5)
    fh.savefig(fname, dpi=1000)
    print("Saved plot", fname)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_file",
        help="The input CSV file.",
    )
    parser.add_argument(
        "output_file_excess_tlat",
        help="The output PDF filename to create for excess tail latency.",
    )
    parser.add_argument(
        "output_file_achievable_load",
        help="The output PDF filename to create for the highest load under SLO.",
    )
    args = parser.parse_args()
    df = pd.read_csv(args.input_file)
    calc_normalized_loads(df)
    plot_excess_tlat(args, df)
    plot_achievable_load(args, df)


if __name__ == "__main__":
    main()
