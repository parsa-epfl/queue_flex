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
from components.bucketed_index import BucketedIndex, is_odd
from .shared_fixtures import simpy_env

import pytest
import simpy


@pytest.fixture(name="bindex", scope="function", params=[7])
def create_bucketed_index(request):
    return BucketedIndex(num_buckets=request.param)


# Writer process that increments a version in the index and succeeds any waiting events after second increment
def writer(env, bindex, bucket, time_log):
    old_val = bindex.inc_index_version(bucket)
    assert is_odd(old_val) is True
    time_log["inc1"] = (old_val, env.now)
    yield env.timeout(4)
    new_val = bindex.inc_index_version(bucket)
    assert is_odd(new_val) is False
    time_log["inc2"] = (new_val, env.now)

    bindex.succeed_event_for_bucket(bucket)
    assert len(bindex.event_waitlist[bucket]) == 0


def poll_reader(env, bindex, bucket, time_log):
    check_string = "check"
    pass_string = "pass"
    yield env.timeout(1)
    n = 1
    while is_odd(bindex.get_index_version(bucket)):
        time_log[check_string + str(n)] = (bindex.get_index_version(bucket), env.now)
        n += 1
        yield env.timeout(1)
    time_log[pass_string] = (bindex.get_index_version(bucket), env.now)


def event_reader(env, bindex, bucket, time_log, suffix):
    while is_odd(bindex.get_index_version(bucket)):
        time_log["check" + suffix] = (bindex.get_index_version(bucket), env.now)
        event = bindex.get_event_for_increment(env, bucket)
        assert event in bindex.event_waitlist[bucket]
        (bucket_index, successful_val) = yield event
    time_log["pass" + suffix] = (successful_val, env.now)


# In this test, we check 2 independent buckets don't generate any 'check' messages
def test_accessor_poll_independent(simpy_env, bindex):
    time_log = {}
    simpy_env.process(writer(simpy_env, bindex, 0, time_log))
    simpy_env.process(poll_reader(simpy_env, bindex, 4, time_log))
    simpy_env.run()

    assert time_log == {
        "inc1": (1, 0),
        "inc2": (2, 4),
        "pass": (0, 1),
    }


# Ensure that a polling reader is blocked until the writer increments back to an even version
def test_accessor_poll_conflict(simpy_env, bindex):
    time_log = {}
    simpy_env.process(writer(simpy_env, bindex, 0, time_log))
    simpy_env.process(poll_reader(simpy_env, bindex, 0, time_log))
    simpy_env.run()

    assert time_log == {
        "inc1": (1, 0),
        "inc2": (2, 4),
        "check1": (1, 1),
        "check2": (1, 2),
        "check3": (1, 3),
        "pass": (2, 4),
    }


# Basic test for a single blocking event reader
def test_accessor_event_single(simpy_env, bindex):
    time_log = {}
    simpy_env.process(writer(simpy_env, bindex, 0, time_log))
    simpy_env.process(event_reader(simpy_env, bindex, 0, time_log, "_t0"))
    simpy_env.run()

    assert time_log == {
        "inc1": (1, 0),
        "inc2": (2, 4),
        "check_t0": (1, 0),
        "pass_t0": (2, 4),
    }


# Multiple event readers in the same bucket
def test_accessor_event_multiple(simpy_env, bindex):
    time_log = {}
    simpy_env.process(writer(simpy_env, bindex, 0, time_log))
    simpy_env.process(event_reader(simpy_env, bindex, 0, time_log, "_t0"))
    simpy_env.process(event_reader(simpy_env, bindex, 0, time_log, "_t1"))
    simpy_env.run()

    assert time_log == {
        "inc1": (1, 0),
        "inc2": (2, 4),
        "check_t0": (1, 0),
        "pass_t0": (2, 4),
        "check_t1": (1, 0),
        "pass_t1": (2, 4),
    }
