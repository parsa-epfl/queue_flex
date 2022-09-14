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
from numpy import linspace, concatenate
from math import ceil

LOW_POINT = 0.05
HIGH_POINT = 1.0


def convert_to_abs(relative_load_point, max_load):
    return float(max_load) / relative_load_point


class RangeMaker(object):
    def __init__(
        self,
        stime_generator,
        num_workers,
        num_points,
        low_high_cutoff,
        concentration=None,
        starting_load=LOW_POINT,
    ):
        self.stime_generator = stime_generator
        self.num_workers = num_workers
        self.num_points = num_points
        self.low_high_cutoff = low_high_cutoff
        self.starting_load = starting_load
        if concentration is None:
            self.noskew = True
        else:
            self.noskew = False
        self.concentration = concentration

    def make_load_range(self):
        if self.noskew:
            rel_range = linspace(self.starting_load, HIGH_POINT, self.num_points)
        else:
            lowpt = self.starting_load
            midpt = self.low_high_cutoff
            num_datapoints_high = ceil(self.num_points * self.concentration)
            num_datapoints_low = self.num_points - num_datapoints_high
            low_range_relative = linspace(lowpt, midpt, num_datapoints_low)
            high_range_relative = linspace(midpt, HIGH_POINT, num_datapoints_high)
            rel_range = concatenate([low_range_relative, high_range_relative])

        #print("*** Range of relative load points",rel_range)
        final_range = [convert_to_abs(x, self.get_max_normpt()) for x in rel_range]
        #print("*** Range of abs. load points",final_range)
        return final_range

    def get_max_normpt(self):
        return float(self.stime_generator.get_exp_stime()) / self.num_workers
