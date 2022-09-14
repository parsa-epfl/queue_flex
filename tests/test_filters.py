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
from components.request_filter_lambdas import reqs_conflict
from components.requests import RPCRequest

import pytest


@pytest.fixture
def read_to_zero():
    return RPCRequest(0, "keyZero", False, 0)


@pytest.fixture
def read_to_one():
    return RPCRequest(0, "keyOne", False, 1)


@pytest.fixture
def write_to_zero():
    return RPCRequest(0, "keyZero", True, 0)


@pytest.fixture
def write_to_one():
    return RPCRequest(0, "keyOne", True, 1)


bm = 100  # not important for now, but do test wraparounds later


def test_no_conflicts(read_to_zero, read_to_one, write_to_zero, write_to_one):
    read_to_zero_again = read_to_zero
    assert reqs_conflict(read_to_zero_again, read_to_zero, bm) == False
    assert reqs_conflict(read_to_zero, read_to_one, bm) == False
    assert reqs_conflict(read_to_zero, write_to_one, bm) == False
    assert reqs_conflict(write_to_zero, write_to_one, bm) == False


def test_conflicts(read_to_zero, read_to_one, write_to_zero, write_to_one):
    write_to_zero_again = write_to_zero
    write_to_one_again = write_to_one
    assert reqs_conflict(read_to_zero, write_to_zero, bm) == True
    assert reqs_conflict(write_to_zero_again, write_to_zero, bm) == True
    assert reqs_conflict(read_to_one, write_to_one, bm) == True
    assert reqs_conflict(write_to_one_again, write_to_one, bm) == True
