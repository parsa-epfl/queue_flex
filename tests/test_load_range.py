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

# Test that the component for making normalized load ranges matches expected lambdas.

from ..components.load_range import RangeMaker
from ..components.mock_stime_gen import MockServiceTimeGenerator
from .shared_fixtures import simpy_env
from numpy import linspace, concatenate

import pytest
import math


@pytest.fixture
def mock_stime_generator():
    return MockServiceTimeGenerator(exp_stime=10.0)


@pytest.fixture
def exp_lowhigh_output():
    # Use 1 worker and 3 points for simplicity, to check evenly spaced output without concentration
    rel_range = [0.05, 0.525, 1.0]
    return [10 / x for x in rel_range]


@pytest.fixture
def exp_concentration_output():
    # Test same max load but this time use concentration, mid point should be 20 (normalized load
    # of 0.5)
    rel_range = [0.05, 0.5, 1.0]
    return [10 / x for x in rel_range]


@pytest.fixture
def exp_manyworkers_output():
    # Use 5 workers, and therefore theoretical max time should be reduced down by 5
    rel_range = [0.05, 0.5, 1.0]
    return [2 / x for x in rel_range]


@pytest.fixture
def exp_nonuniform_output():
    # Use 5 workers again, but this time request 10 points, with 7 pts above load of 0.8
    mid = 0.8
    from numpy import linspace, concatenate

    low_range = linspace(0.05, 0.8, 3)
    high_range = linspace(0.8, 1.0, 7)
    rel_range = concatenate([low_range, high_range])
    return [2 / x for x in rel_range]


def check_ranges_match(exp_range, test_range):
    assert len(exp_range) == len(test_range)
    for i in range(len(exp_range)):
        assert math.isclose(exp_range[i], test_range[i], rel_tol=1e-2)


def test_range_maker_lowhigh(mock_stime_generator, simpy_env, exp_lowhigh_output):
    rangeMaker = RangeMaker(
        mock_stime_generator, num_workers=1, num_points=3, low_high_cutoff=0.5
    )
    final_range = rangeMaker.make_load_range()
    check_ranges_match(exp_lowhigh_output, final_range)


def test_range_maker_concentration(
    mock_stime_generator, simpy_env, exp_concentration_output
):
    rangeMaker = RangeMaker(
        mock_stime_generator,
        num_workers=1,
        num_points=3,
        low_high_cutoff=0.5,
        concentration=0.5,
    )
    final_range = rangeMaker.make_load_range()
    check_ranges_match(exp_concentration_output, final_range)


def test_range_maker_manyworkers(
    mock_stime_generator, simpy_env, exp_manyworkers_output
):
    rangeMaker = RangeMaker(
        mock_stime_generator,
        num_workers=5,
        num_points=3,
        low_high_cutoff=0.5,
        concentration=0.5,
    )
    final_range = rangeMaker.make_load_range()
    check_ranges_match(exp_manyworkers_output, final_range)


def test_range_maker_nonuniform(mock_stime_generator, simpy_env, exp_nonuniform_output):
    rangeMaker = RangeMaker(
        mock_stime_generator,
        num_workers=5,
        num_points=10,
        low_high_cutoff=0.8,
        concentration=0.7,
    )
    final_range = rangeMaker.make_load_range()
    check_ranges_match(exp_nonuniform_output, final_range)
