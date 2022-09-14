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
# Helper functions for pandas dataframes.
# Mark Sutherland
# (C) 2021
import pandas
import numpy


def get_xy_ranges(df, x_label, y_label, x_interval, y_interval):
    """Function to go through a dataframe and return two ranges, for x and y axis plotting."""
    x_min = df[x_label].min()
    x_max = df[x_label].max()
    y_min = df[y_label].min()
    y_max = df[y_label].max()

    x_range = numpy.arange(start=x_min, stop=x_max + x_interval, step=x_interval)
    y_range = numpy.arange(start=y_min, stop=y_max + y_interval, step=y_interval)
    return x_range, y_range
