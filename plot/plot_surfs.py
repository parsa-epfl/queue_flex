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
# Takes a csv file containing throughput/latency data of zipf/write fractions, and plot two surfaces:
#   - excess tail latency, and throughput compared to ideal.
# Mark Sutherland
# (C) 2021

import argparse
import os
import matplotlib as mpl

mpl.use("Agg")
# mpl.rc("font", **{"size": 22, "family": "serif"})
mpl.rc("font", **{"size": 26})
mpl.rcParams["axes.labelpad"] = 30
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42
# mpl.rcParams['xtick.major.pad'] = 5
# mpl.rcParams['ytick.major.pad'] = 5
# mpl.rcParams['ztick.major.pad'] = 5
from matplotlib import cm
import numpy
import pandas as pd
from matplotlib import pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from scipy.interpolate import griddata
import sys
sys.path.append('.') # to solve relative import, only works running $ python plot/plot_surfs.py
from util.string_ops import conv_file_suffix
from util.dataframe_helpers import get_xy_ranges

plt.style.use("grayscale")

colorbar_ticks_tput = [0, 0.2, 0.4, 0.6, 0.8, 1]
colorbar_ticks_tlat = [2, 4, 6, 8, 10]
key_to_plot = "tput_slowdown"
key_to_title = {
    "excess_tlat": "p99 Norm. to Ideal",
    "tput_slowdown": "Tput. @ SLO",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("ifile_base", help="CSV file containing the baseline.")
    parser.add_argument(
        "ifile_comp", help="CSV file containing the compaction results."
    )
    args = parser.parse_args()

    ## Generate a synthetic dataframe to plot for now.
    df_base = pd.read_csv(args.ifile_base)
    df_comp = pd.read_csv(args.ifile_comp)

    fig = plt.figure()
    ax = fig.add_subplot(111, projection="3d")

    ## Make 2dXY and plot base
    print("Plotting baseline figure....")
    x_range, y_range = get_xy_ranges(
        df_base, "wr_frac", "zipf", x_interval=5, y_interval=0.1
    )
    X, Y = numpy.meshgrid(x_range, y_range)

    Z = griddata(
        (df_base["wr_frac"], df_base["zipf"]),
        # (df_base["zipf"], df_base["wr_frac"]),
        df_base[key_to_plot],
        (X, Y),
        method="cubic",
    )
    surf = ax.plot_surface(
        X,
        Y,
        Z,
        rstride=1,
        cstride=1,
        cmap=cm.coolwarm,
        linewidth=0,
        antialiased=False,
        vmin=0,
        vmax=1,
    )
    ax.set_zlim(0, 1)
    ax.set_xlabel("Wr. Frac.")
    ax.set_ylabel("Zipf. Coeff")
    ax.set_zlabel(key_to_title[key_to_plot])
    ax.set_yticks([1, 1.2, 1.4])
    ax.set_xticks([0, 20, 40, 60])
    # ax.set_zticks([0, 0.25, 0.5, 0.75, 1])
    ax.set_zticks([0, 0.5, 1])
    # ax.set_ylim(1.4,0.9)
    ax.tick_params(axis="z", **{"pad": 12})
    """
    cbar = fig.colorbar(
        surf,
        location="right",
        ax=ax,
        ticks=colorbar_ticks_tput,
        pad=0.2,
        anchor=(0.5, 0.25),
        shrink=0.7,
    )
    """
    fig_adjust_sizes = dict(left=-0.15, right=1.05, top=1.05, bottom=0.15)
    # fig_adjust_sizes = dict(left=0, right=1, top=1.2, bottom=-0.05)
    fig.subplots_adjust(**fig_adjust_sizes)
    fig.savefig(conv_file_suffix(args.ifile_base, "pdf"))

    ## Make XY and plot comp
    print("Plotting compaction figure....")
    fig = plt.figure()
    ax = fig.add_subplot(111, projection="3d")
    Z_comp = griddata(
        (df_comp["wr_frac"], df_comp["zipf"]),
        df_comp[key_to_plot],
        (X, Y),
        method="cubic",
    )
    comp_surf = ax.plot_surface(
        X,
        Y,
        Z_comp,
        rstride=1,
        cstride=1,
        cmap=cm.coolwarm,
        vmin=0,
        vmax=1,
        linewidth=0,
        antialiased=False,
    )
    ax.set_zlim(0, 1)
    ax.set_xlabel("Wr. Frac.")
    ax.set_ylabel("Zipf. Coeff")
    ax.set_zlabel(key_to_title[key_to_plot])
    # ax.set_zticks([0, 0.25, 0.5, 0.75, 1])
    ax.set_zticks([0, 0.5, 1])
    ax.set_yticks([1, 1.2, 1.4])
    ax.set_xticks([0, 20, 40, 60])
    ax.tick_params(axis="z", **{"pad": 12})
    cbar = fig.colorbar(
        comp_surf,
        location="right",
        ax=ax,
        ticks=colorbar_ticks_tput,
        pad=0.2,
        anchor=(0.5, 0.25),
        shrink=0.7,
    )

    fig_adjust_sizes = dict(left=0, right=1, top=1.2, bottom=0)
    fig.subplots_adjust(**fig_adjust_sizes)
    fig.savefig(conv_file_suffix(args.ifile_comp, "pdf"))


if __name__ == "__main__":
    main()
