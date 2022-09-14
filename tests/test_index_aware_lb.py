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
from components.requests import RPCRequest, PullFeedbackRequest
from components.end_measure import EndOfMeasurements
from components.dispatch_policies.key_based_policies import CREWDispatchPolicy
from components.load_balancer import IndexAwareLoadBalancer
from components.bucketed_index import is_odd
from .shared_fixtures import (
    simpy_env,
    simpy_env_queues,
    create_bucketed_index,
    reqtrace_RW_50R,
    reqtrace_2R2W_diff,
    reqtrace_2W2R_allconf,
    reqtrace_2W6R_diff_buckets,
    reqtrace_1W7R_allconf,
    reqtrace_8R,
)
from .util import make_comm_channel

import pytest
import simpy


class MockLoadGenerator:
    def __init__(self, simpy_env, out_queue, interarrival_time):
        self.env = simpy_env
        self.out_queue = out_queue
        self.req_trace = []
        self.interarrival_time = interarrival_time
        self.action = self.env.process(self.gen_load())

    def gen_load(self):
        for r in self.req_trace:
            self.out_queue.put(r)
            yield self.env.timeout(self.interarrival_time)
        # self.out_queue.put(EndOfMeasurements())

    def set_req_trace(self, requests):
        self.req_trace = requests


def event_consumer(env, queue, my_index, event_log):
    while True:
        try:
            req = yield queue.get()
            event_id = "req_" + str(req.getID())
            event_log[event_id] = (env.now, my_index)
            yield env.timeout(10)
        except simpy.Interrupt as i:
            print("Ending consumer processing")
            break


def index_writer_reader(env, queue, my_id, time_log, bindex, pull_queue):
    while True:
        req = yield queue.get()
        event_id = "req_" + str(req.getID())
        bucket = hash(req) % bindex.get_num_buckets()
        if req.getWrite():
            old_val = bindex.inc_index_version(bucket)
            assert is_odd(old_val) is True
            time_log[event_id + "_inc1"] = (old_val, env.now)
            yield env.timeout(100)
            new_val = bindex.inc_index_version(bucket)
            assert is_odd(new_val) is False
            time_log[event_id + "_inc2"] = (new_val, env.now)
            bindex.succeed_event_for_bucket(bucket)
            assert len(bindex.event_waitlist[bucket]) == 0
        else:
            check_string = "check"
            pass_string = "pass"
            while is_odd(bindex.get_index_version(bucket)):
                time_log[check_string + "_" + event_id] = (my_id, env.now)
                yield env.timeout(50)
            time_log[pass_string + "_" + event_id] = (my_id, env.now)
            yield env.timeout(50)
        pull_queue.put(PullFeedbackRequest(my_id, req))


@pytest.fixture(
    name="setup_generator_crewbalancer",
)
def create_env_for_test(simpy_env_queues, bindex):
    simpy_env = simpy_env_queues["env"]
    queues = simpy_env_queues["queues"]
    input_queue = make_comm_channel(simpy_env, 10)
    pull_queue = make_comm_channel(simpy_env, 10)

    lgen = MockLoadGenerator(simpy_env, input_queue, 10)
    dp = CREWDispatchPolicy(len(queues), queues, num_buckets=100000000)

    lb = IndexAwareLoadBalancer(
        simpy_env=simpy_env,
        lgen_to_interrupt=lgen,
        in_queue=input_queue,
        disp_queues=queues,
        index_obj=bindex,
        pull_queue=pull_queue,
        dp=dp,
    )
    return {
        "env": simpy_env,
        "queues": queues,
        "lgen": lgen,
        "lb": lb,
        "bindex": bindex,
        "pull_queue": pull_queue,
    }


def test_lb_creation_run(setup_generator_crewbalancer):
    simpy_env = setup_generator_crewbalancer["env"]
    queues = setup_generator_crewbalancer["queues"]
    lgen = setup_generator_crewbalancer["lgen"]
    lb = setup_generator_crewbalancer["lb"]
    simpy_env.run()


@pytest.mark.parametrize(
    "simpy_env_queues",
    [4],
    indirect=True,
)
@pytest.mark.parametrize(
    "bindex",
    [2],
    indirect=True,
)
def test_lb_passes_all_events(setup_generator_crewbalancer, reqtrace_8R):
    simpy_env = setup_generator_crewbalancer["env"]
    queues = setup_generator_crewbalancer["queues"]
    lgen = setup_generator_crewbalancer["lgen"]
    lb = setup_generator_crewbalancer["lb"]
    bindex = setup_generator_crewbalancer["bindex"]
    pull_queue = setup_generator_crewbalancer["pull_queue"]
    log = {}

    lgen.set_req_trace(reqtrace_8R)

    for i in range(len(queues)):
        simpy_env.process(event_consumer(simpy_env, queues[i], i, log))
    simpy_env.run()

    assert log == {
        "req_0": (10, 0),
        "req_1": (20, 1),
        "req_2": (30, 2),
        "req_3": (40, 3),
        "req_4": (50, 0),
        "req_5": (60, 1),
        "req_6": (70, 2),
        "req_7": (80, 3),
    }


@pytest.mark.parametrize(
    "simpy_env_queues",
    [4],
    indirect=True,
)
@pytest.mark.parametrize(
    "bindex",
    [2],
    indirect=True,
)
def test_index_lb_holds_dep_reads(setup_generator_crewbalancer, reqtrace_1W7R_allconf):
    simpy_env = setup_generator_crewbalancer["env"]
    queues = setup_generator_crewbalancer["queues"]
    lgen = setup_generator_crewbalancer["lgen"]
    lb = setup_generator_crewbalancer["lb"]
    bindex = setup_generator_crewbalancer["bindex"]
    pull_queue = setup_generator_crewbalancer["pull_queue"]
    assert bindex.get_num_buckets() == 2

    lgen.set_req_trace(reqtrace_1W7R_allconf)

    log = {}
    for i in range(len(queues)):
        simpy_env.process(
            index_writer_reader(simpy_env, queues[i], i, log, bindex, pull_queue)
        )
    simpy_env.run()

    assert log == {
        "req_0_inc1": (1, 10),
        "req_0_inc2": (2, 110),
        "pass_req_1": (0, 120),
        "pass_req_2": (1, 120),
        "pass_req_3": (2, 120),
        "pass_req_4": (3, 120),
        "pass_req_5": (0, 170),
        "pass_req_6": (1, 170),
        "pass_req_7": (2, 170),
    }


@pytest.mark.parametrize(
    "simpy_env_queues",
    [4],
    indirect=True,
)
@pytest.mark.parametrize(
    "bindex",
    [2],
    indirect=True,
)
def test_index_lb_multiple_wrchains(
    setup_generator_crewbalancer, reqtrace_2W6R_diff_buckets
):
    simpy_env = setup_generator_crewbalancer["env"]
    queues = setup_generator_crewbalancer["queues"]
    lgen = setup_generator_crewbalancer["lgen"]
    lb = setup_generator_crewbalancer["lb"]
    bindex = setup_generator_crewbalancer["bindex"]
    pull_queue = setup_generator_crewbalancer["pull_queue"]
    assert bindex.get_num_buckets() == 2

    lgen.set_req_trace(reqtrace_2W6R_diff_buckets)

    log = {}
    for i in range(len(queues)):
        simpy_env.process(
            index_writer_reader(simpy_env, queues[i], i, log, bindex, pull_queue)
        )
    simpy_env.run()

    assert log == {
        "req_0_inc1": (1, 10),
        "req_0_inc2": (2, 110),
        "req_4_inc1": (1, 50),
        "req_4_inc2": (2, 150),
        "pass_req_1": (0, 120),
        "pass_req_2": (2, 120),
        "pass_req_3": (3, 120),
        "pass_req_5": (1, 160),
        "pass_req_6": (0, 170),
        "pass_req_7": (1, 210),
    }


@pytest.mark.parametrize(
    "simpy_env_queues",
    [4],
    indirect=True,
)
@pytest.mark.parametrize(
    "bindex",
    [2],
    indirect=True,
)
def test_index_lb_singlechain_multiwrite(
    setup_generator_crewbalancer, reqtrace_2W2R_allconf
):
    simpy_env = setup_generator_crewbalancer["env"]
    queues = setup_generator_crewbalancer["queues"]
    lgen = setup_generator_crewbalancer["lgen"]
    lb = setup_generator_crewbalancer["lb"]
    bindex = setup_generator_crewbalancer["bindex"]
    pull_queue = setup_generator_crewbalancer["pull_queue"]
    assert bindex.get_num_buckets() == 2

    lgen.set_req_trace(reqtrace_2W2R_allconf)

    log = {}
    for i in range(len(queues)):
        simpy_env.process(
            index_writer_reader(simpy_env, queues[i], i, log, bindex, pull_queue)
        )
    simpy_env.run()

    assert log == {
        "req_0_inc1": (1, 10),
        "req_0_inc2": (2, 110),
        "pass_req_1": (0, 120),
        "req_2_inc1": (3, 180),
        "req_2_inc2": (4, 280),
        "pass_req_3": (0, 290),
    }


@pytest.mark.parametrize(
    "simpy_env_queues",
    [4],
    indirect=True,
)
@pytest.mark.parametrize(
    "bindex",
    [2],
    indirect=True,
)
def test_index_lb_reads_block(setup_generator_crewbalancer, reqtrace_2R2W_diff):
    simpy_env = setup_generator_crewbalancer["env"]
    queues = setup_generator_crewbalancer["queues"]
    lgen = setup_generator_crewbalancer["lgen"]
    lb = setup_generator_crewbalancer["lb"]
    bindex = setup_generator_crewbalancer["bindex"]
    pull_queue = setup_generator_crewbalancer["pull_queue"]
    assert bindex.get_num_buckets() == 2

    lgen.set_req_trace(reqtrace_2R2W_diff)

    log = {}
    for i in range(len(queues)):
        simpy_env.process(
            index_writer_reader(simpy_env, queues[i], i, log, bindex, pull_queue)
        )
    simpy_env.run()

    assert log == {
        "pass_req_0": (0, 10),
        "pass_req_1": (1, 20),
        "req_2_inc1": (1, 70),
        "req_2_inc2": (2, 170),
        "req_3_inc1": (1, 80),
        "req_3_inc2": (2, 180),
    }


@pytest.mark.parametrize(
    "simpy_env_queues",
    [4],
    indirect=True,
)
@pytest.mark.parametrize(
    "bindex",
    [2],
    indirect=True,
)
def test_no_write_starvation(setup_generator_crewbalancer, reqtrace_RW_50R):
    simpy_env = setup_generator_crewbalancer["env"]
    queues = setup_generator_crewbalancer["queues"]
    lgen = setup_generator_crewbalancer["lgen"]
    lb = setup_generator_crewbalancer["lb"]
    bindex = setup_generator_crewbalancer["bindex"]
    pull_queue = setup_generator_crewbalancer["pull_queue"]
    assert bindex.get_num_buckets() == 2

    lgen.set_req_trace(reqtrace_RW_50R)

    log = {}
    for i in range(len(queues)):
        simpy_env.process(
            index_writer_reader(simpy_env, queues[i], i, log, bindex, pull_queue)
        )
    simpy_env.run()

    assert log["req_1_inc1"] == (1, 70)
    assert log["req_1_inc2"] == (2, 170)
    assert log["pass_req_2"] == (0, 180)
