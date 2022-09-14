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
from ..components.requests import RPCRequest
from ..components.dispatch_policies.key_based_policies import (
    EREWDispatchPolicy,
    CREWDispatchPolicy,
)
from .shared_fixtures import simpy_env, simpy_env_store

import pytest
import simpy
import hashlib

num_queues = 16
k_list = [1234, 4321, "theKey", "anotherKey"]
wr_list1 = [False, True, True, False]
wr_list2 = [True, False, False, True]
wr_list_all = [True] * 4
kw_pair1 = {"k_list": k_list, "wr_list": wr_list1}
kw_pair2 = {"k_list": k_list, "wr_list": wr_list2}
kw_pair3 = {"k_list": k_list, "wr_list": wr_list_all}

# Make a mixture of reads/writes
num_repeated_reads = 12
k_list_reads = k_list + ["aRepeatedKey"] * num_repeated_reads
wr_list_reads = wr_list_all + [False] * num_repeated_reads
kw_pair_reads = {"k_list": k_list_reads, "wr_list": wr_list_reads}


def get_smallest_elem(l):
    smallest = 1000000000000000000
    idx = 0
    for i in range(len(l)):
        cur_len = l[i]
        if cur_len < smallest:
            smallest = cur_len
            idx = i
    return idx


def conv_k_to_q(k):
    h_obj = hashlib.sha256()
    h_obj.update(bytes(str(k).encode("utf-8")))
    exp_keyhash = h_obj.hexdigest()[-16:-8]
    qout = int(exp_keyhash, base=16) % num_queues
    return qout


def expected_q_list_erew(param):
    k_list = param["k_list"]
    wr_list = param["wr_list"]

    odict = {}
    num = 0
    for k in k_list:
        odict[(k, num)] = conv_k_to_q(k)
        num += 1
    return odict


def expected_q_list_crew(param):
    k_list = param["k_list"]
    wr_list = param["wr_list"]
    q_len_counters = [0] * num_queues
    odict = {}
    num = 0
    for i in range(len(k_list)):
        k = k_list[i]
        if wr_list[i] is True:
            qdx = conv_k_to_q(k)
        else:
            qdx = get_smallest_elem(q_len_counters)
        odict[(k, num)] = qdx
        q_len_counters[qdx] += 1
        num += 1
    return odict


def req_maker(env, store, kwlist):
    r_num = 0
    k_list = kwlist["k_list"]
    wr_list = kwlist["wr_list"]
    for i in range(len(k_list)):
        r = RPCRequest(r_num, k_list[i], wr_list[i])
        store.put(r)
        r_num += 1
        yield env.timeout(1)


def dispatcher(env, dp, store, q_log, num_to_dispatch):
    num_disp = 0
    while num_disp < num_to_dispatch:
        req = yield store.get()
        q_idx = dp.select(req)
        assert q_idx < num_queues
        q_log[(req.key, num_disp)] = q_idx
        num_disp += 1


@pytest.fixture(name="sim_erew", params=[kw_pair1, kw_pair2])
def run_erew_with_kwinput(simpy_env_store, request):
    simpy_env = simpy_env_store["env"]
    store = simpy_env_store["store"]

    queue_selection_log = {}
    dp = EREWDispatchPolicy(num_queues, [store], num_buckets=10000000000)
    simpy_env.process(req_maker(simpy_env, store, request.param))
    simpy_env.process(
        dispatcher(
            simpy_env, dp, store, queue_selection_log, len(request.param["k_list"])
        )
    )
    simpy_env.run()
    return {"param": request.param, "output": queue_selection_log}


@pytest.fixture(name="sim_crew_writes", params=[kw_pair3])
def run_crew_with_writes(simpy_env_store, request):
    simpy_env = simpy_env_store["env"]
    store = simpy_env_store["store"]

    queue_selection_log = {}
    dp = CREWDispatchPolicy(num_queues, [store], num_buckets=10000000000)
    simpy_env.process(req_maker(simpy_env, store, request.param))
    simpy_env.process(
        dispatcher(
            simpy_env, dp, store, queue_selection_log, len(request.param["k_list"])
        )
    )
    simpy_env.run()
    return {"param": request.param, "output": queue_selection_log}


@pytest.fixture(name="sim_crew_wr_read", params=[kw_pair_reads])
def run_crew_with_mix(simpy_env_store, request):
    simpy_env = simpy_env_store["env"]
    store = simpy_env_store["store"]

    queue_selection_log = {}
    dp = CREWDispatchPolicy(num_queues, [store], num_buckets=10000000000)
    simpy_env.process(req_maker(simpy_env, store, request.param))
    simpy_env.process(
        dispatcher(
            simpy_env, dp, store, queue_selection_log, len(request.param["k_list"])
        )
    )
    simpy_env.run()
    return {"param": request.param, "output": queue_selection_log}


# Runs the entire simulation twice to check that queues match, and that
# the different write list is not important
def test_erew_hashing(sim_erew):
    expected_odict = expected_q_list_erew(sim_erew["param"])
    assert sim_erew["output"] == expected_odict


# Verify crew policy hashes writes just like EREW does
def test_crew_hashing(sim_crew_writes):
    expected_odict = expected_q_list_erew(sim_crew_writes["param"])
    assert sim_crew_writes["output"] == expected_odict


# Test that reads are balanced across all queues
def test_crew_spreads_reads(sim_crew_wr_read):
    final_out = expected_q_list_crew(kw_pair_reads)
    assert sim_crew_wr_read["output"] == final_out
