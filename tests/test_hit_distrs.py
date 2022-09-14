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

#  Test the rollHit function's accuracy as its used by many components.
from components.load_generator import rollHit
from collections import Counter
from math import ceil
import random

import simpy
import pytest


@pytest.fixture(params=[50, 95, 22.38, 0.0])
def setup_prob_for_test(request):
    return request.param


def within_xpercent(number, exp, x_error):
    low_range = exp - exp * (float(x_error) / 100)
    high_range = exp + exp * (float(x_error) / 100)
    # print('low',low_range,'high',high_range)
    # print('expected',exp,'got',number)
    if number >= low_range and number <= high_range:
        return True
    return False

def test_hit_deterministic(setup_prob_for_test):
    num_vals = 1000
    random.seed(0xdeadbeef)

    starting_rands = [rollHit(setup_prob_for_test) for i in range(num_vals)]

    # N times, reseed and check the randoms are equal
    N = 10
    for n in range(N):
        random.seed(0xdeadbeef)
        compare_rands = [rollHit(setup_prob_for_test) for i in range(num_vals)]
        assert compare_rands == starting_rands

# Test methodology: generate N random numbers from a given success distribution, test that
# the returned number of True/Falses is within 1% of expected
def test_rollHit(setup_prob_for_test):
    num_vals = 1000000
    prob_true = setup_prob_for_test  # percent

    rands = [rollHit(prob_true) for i in range(num_vals)]
    c_vals = Counter(rands)

    exp_true = ceil(num_vals * (float(prob_true) / 100))
    exp_false = num_vals - exp_true

    assert within_xpercent(c_vals[True], exp_true, 1) is True
    assert within_xpercent(c_vals[False], exp_false, 1) is True
