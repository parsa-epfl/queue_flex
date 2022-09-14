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
# Author: Mark Sutherland, (C) 2021
from components.dispatch_policies.JBSQ import (
    JBSCREWDispatchPolicy,
    DynJBSCREWDispatchPolicy,
)
from .shared_fixtures import (
    simpy_env,
    simpy_env_queues,
    reqtrace_2R2W_diff,
    reqtrace_2W6R_diff_buckets,
    reqtrace_1W3R_2W2R_diff_buckets
)
import pytest
import simpy


@pytest.fixture
def jbscrew_policy(simpy_env, request):
    synthetic_num_buckets = 10000
    queues = [
        simpy.Store(simpy_env) for i in range(request.param)
    ]  # param is the number of queues
    return JBSCREWDispatchPolicy(
        simpy_env, queues, request.param, num_hash_buckets=synthetic_num_buckets
    )


@pytest.fixture
def dyn_jbscrew_policy(simpy_env, request):
    synthetic_num_buckets = 10000
    queues = [
        simpy.Store(simpy_env) for i in range(request.param)
    ]  # param is the number of queues

    # Assume we can track 64 exclusive buckets simultaneously
    simul_excl_buckets = 64

    return DynJBSCREWDispatchPolicy(
        simpy_env,
        queues,
        request.param,
        num_hash_buckets=synthetic_num_buckets,
        max_buckets_tracking=simul_excl_buckets,
    )


@pytest.mark.parametrize(
    "jbscrew_policy",
    [4, pytest.param(0, marks=pytest.mark.xfail(raises=ValueError))],
    indirect=True,
)
def test_jbscrew_init(jbscrew_policy):
    assert jbscrew_policy is not None


@pytest.mark.parametrize("jbscrew_policy", [2], indirect=True)
def test_jbscrew_lb(jbscrew_policy, reqtrace_2R2W_diff):
    # Test that selecting a queue for the first two reqs gives queues 0 and 1.
    subset = reqtrace_2R2W_diff[:2]
    queue_expected = [i for i in range(2)]
    it = zip(subset, queue_expected)
    for req, q_exp in it:
        assert jbscrew_policy.select(req) == q_exp

    # Same thing again
    it = zip(subset, queue_expected)  # remake because iterator is exhausted
    for req, q_exp in it:
        assert jbscrew_policy.select(req) == q_exp

    # Test that dispatch fails because queues are full
    queue_expected = [-1 for i in range(2)]
    it = zip(subset, queue_expected)  # remake because iterator is exhausted
    for req, q_exp in it:
        assert jbscrew_policy.select(req) == q_exp

    # Signal queue 1 popped a req
    jbscrew_policy.func_executed(c_id=1, f_type=None)
    # Signal queue 0 popped a req
    jbscrew_policy.func_executed(c_id=0, f_type=None)

    subset_writes = reqtrace_2R2W_diff[2:]
    req = subset_writes[0]

    # Check now all disps function.
    queue_expected = [i for i in range(2)]
    it_wr = zip(subset_writes, queue_expected)
    for req, q_exp in it_wr:
        assert jbscrew_policy.select(req) == q_exp


@pytest.mark.parametrize("dyn_jbscrew_policy", [4], indirect=True)
def test_dyn_jbscrew_exclusivity(dyn_jbscrew_policy, reqtrace_1W3R_2W2R_diff_buckets):
    # Test that the first writer request goes to queue 0, and sets the exclusive sticky.
    # All reads after it should go to the other queues.
    subset = reqtrace_1W3R_2W2R_diff_buckets[:4]
    queue_expected = [i for i in range(4)]

    it = zip(subset, queue_expected)
    for req, q_exp in it:
        assert dyn_jbscrew_policy.select(req) == q_exp
        if req.getWrite():
            bucket = hash(req) % dyn_jbscrew_policy.num_hash_buckets
            assert bucket in dyn_jbscrew_policy.bucket_mappings

    # Signal that all the queues finished.
    for c in range(4):
        dyn_jbscrew_policy.func_executed(c_id=c, f_type=None)
        if c == 0:
            assert dyn_jbscrew_policy.write_req_finished(bucket=c, excl_q=c) == 0

    # Assert that core 0 cleared the exclusivity sticky.
    assert 0 not in dyn_jbscrew_policy.bucket_mappings

    # Do the same for the 2nd subset of reqs
    len_subset = 4
    subset = reqtrace_1W3R_2W2R_diff_buckets[len_subset:]
    queue_expected = [0,0,1,2]
    it = zip(subset, queue_expected)
    for req, q_exp in it:
        assert dyn_jbscrew_policy.select(req) == q_exp
        if req.getWrite():
            bucket = hash(req) % dyn_jbscrew_policy.num_hash_buckets
            assert bucket in dyn_jbscrew_policy.bucket_mappings
