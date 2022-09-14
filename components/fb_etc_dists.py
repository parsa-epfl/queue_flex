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
## Classes which model the key and value size distributions given in Facebook's seminal ETC workload
## (Workload Analysis of a Large-Scale Key-Value Store, Atikoglu SIGMETRICS'12)

from scipy.stats import genextreme as gev
from scipy.stats import genpareto as gpar
from math import ceil
from bisect import bisect_right

import numpy as np
import random


class ETCKeyDistribution:
    """
    This class models the size distribution of keys in Facebook's ETC workload.
    """

    def __init__(self):
        """Initialize the self.distribution object with the parameters specified above."""
        # Data parameters from the paper
        self.mu = 30.7984
        self.sigma = 8.20449
        self.k = 0.078688
        self.distribution = gev(c=self.k, loc=self.mu, scale=self.sigma)

    def get_random_vars(self, num_vars):
        """Get num_vars random variables from the underlying distribution."""
        return self.distribution.rvs(size=num_vars)

    def pdf(self, x):
        """Return the PDF at the given value of x."""
        y = self.distribution.pdf(x)
        return y


class ETCValueDistribution:
    """
    This class models the size distribution of values in Facebook's ETC workload.
    """

    def __init__(self):
        """Initialize the self.distribution object with the above parameters."""
        # Data parameters from the paper
        self.theta = 0
        self.sigma = 214.476
        self.k = 0.348238

        # Magic table for first 15 lengths
        self.low_pdf_table = {
            0: 0.00536,
            1: 0.00047,
            2: 0.17820,
            3: 0.09239,
            4: 0.00018,
            5: 0.02740,
            6: 0.00065,
            7: 0.00606,
            8: 0.00023,
            9: 0.00837,
            10: 0.00837,
            11: 0.08989,
            12: 0.00092,
            13: 0.00326,
            14: 0.01980,
        }

        self.distribution = gpar(c=self.k, loc=self.theta, scale=self.sigma)
        self.cumsum_low = 0.0
        for v in self.low_pdf_table.values():
            self.cumsum_low += v

        self.low_cdf_table = [0 for i in range(len(self.low_pdf_table))]
        running_sum = 0.0
        for k, v in sorted(self.low_pdf_table.items()):
            running_sum += v
            self.low_cdf_table[k] = running_sum

    def get_random_vars(self, num_vars):
        """Get num_vars random variables from the underlying distribution."""
        out_vars = []

        num_low_vars = ceil(num_vars * self.cumsum_low)
        num_high_vars = num_vars - num_low_vars

        # in the low-range, return numbers based on placing pseudo-random numbers into the cdf
        max_rand_int = self.low_cdf_table[-1] * 10000
        for i in range(num_low_vars):
            r = random.randint(0, max_rand_int)
            r_scaled = r / 10000.0
            val = self.low_cdf_table.bisect_right(r_scaled)
            if val >= len(self.low_cdf_table):
                raise ValueError(
                    "Error when generating low-range values. r_scaled =",
                    r_scaled,
                    "val returned = ",
                    val,
                    "which was beyond the size of the CDF table. It only has the following num entries:",
                    len(self.low_cdf_table),
                )
            out_vars.append(val)

        # In the high range, use the distribution directly
        out_vars.append(self.distribution.rvs(size=num_high_vars))
        return out_vars

    def pdf(self, x):
        """Return the PDF at an integer value of x."""
        if isinstance(x, np.ndarray):
            x_int = x.astype(int)
            x_arg = x_int[0]
            if np.less(x, 15.0):
                y = self.low_pdf_table[x_arg]
            else:
                y = self.distribution.pdf(x)
        else:
            if x < 15:
                y = self.low_pdf_table[x]
            else:
                y = self.distribution.pdf(x)
        return y
