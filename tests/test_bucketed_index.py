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
from components.bucketed_index import BucketedIndex, AsyncIndexUpdater
from .shared_fixtures import simpy_env, create_bucketed_index

import pytest
import simpy


def test_bucket_init(bindex):
    for i in range(bindex.get_num_buckets()):
        assert bindex.get_index_version(bucket_num=i) == 0


def test_bucket_increment(bindex):
    for i in range(bindex.get_num_buckets()):
        assert bindex.inc_index_version(bucket_num=i) == 1


INDEX_UPDATE_LAT = 20


def bucket_updater_proc(env, bucket_object, index_to_update, time_log):
    yield AsyncIndexUpdater(
        env, bucket_object, index_to_update, INDEX_UPDATE_LAT
    ).action
    time_log[index_to_update] = env.now


def updater_forker(simpy_env, bindex, time_log, expected_log):
    yield simpy_env.timeout(1)
    procs = []
    for i in range(bindex.get_num_buckets()):
        expected_update_time = simpy_env.now + INDEX_UPDATE_LAT
        expected_log[i] = expected_update_time
        procs.append(
            simpy_env.process(bucket_updater_proc(simpy_env, bindex, i, time_log))
        )
        yield simpy_env.timeout(1)

    for p in procs:
        yield p

    return (time_log, expected_log)


def test_async_updater(simpy_env, bindex):
    time_log = {}
    expected_log = {}
    simpy_env.process(updater_forker(simpy_env, bindex, time_log, expected_log))
    simpy_env.run()

    for b in range(bindex.get_num_buckets()):
        assert bindex.get_index_version(b) == 1

    assert time_log == expected_log
