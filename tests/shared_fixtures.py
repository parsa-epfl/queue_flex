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

# Shared fixtures for testing any components depending on simpy

import pytest
import simpy
from components.bucketed_index import BucketedIndex
from components.requests import RPCRequest


@pytest.fixture
def simpy_env():
    """Returns a simple simpy environment."""
    return simpy.Environment()


@pytest.fixture
def simpy_env_store(simpy_env):
    """Return a dictionary containing a simple simpy env and a store created on that environment.

    Dictionary format: {\"env\": <env object>, \"store\": <store object>}
    """
    return {"env": simpy_env, "store": simpy.Store(simpy_env)}


@pytest.fixture(
    scope="function",
    params=[1],
)
def simpy_env_queues(simpy_env, request):
    """Return a dictionary containing a simpy env and a list of queues created on that environment.

    Dictionary format: {\"env\": <env object>, \"queues\": [<queue>,...,<queue>]}
    """
    the_env = simpy_env
    queues = [
        simpy.Store(the_env) for i in range(request.param)
    ]  # param is the number of queues
    return {"env": the_env, "queues": queues}


@pytest.fixture(name="bindex", scope="function", params=[7, 1000])
def create_bucketed_index(request):
    return BucketedIndex(num_buckets=request.param)


@pytest.fixture
def reqtrace_2W6R_diff_buckets():
    """Build a trace of 2 writes, with 3 dependent reads each, to different buckets."""
    rq = [RPCRequest(0, "theKey", True, predef_hash=0)]
    for i in range(3):
        rq.append(RPCRequest(1 + i, "theKey", False, predef_hash=0))

    rq.append(RPCRequest(4, "anotherKey", True, predef_hash=1))
    for i in range(3):
        rq.append(RPCRequest(5 + i, "anotherKey", False, predef_hash=1))
    return rq

@pytest.fixture
def reqtrace_1W3R_2W2R_diff_buckets():
    """Build a trace of 1 write, with 3 dependent reads, and 2 writes with 2 reads, respectively."""
    rq = [RPCRequest(0, "theKey", True, predef_hash=0)]
    for i in range(3):
        rq.append(RPCRequest(1 + i, "theKey", False, predef_hash=0))

    rq.append(RPCRequest(4, "anotherKey", True, predef_hash=1))
    rq.append(RPCRequest(5, "anotherKey", True, predef_hash=1))
    for i in range(2):
        rq.append(RPCRequest(6 + i, "anotherKey", False, predef_hash=1))
    return rq


@pytest.fixture
def reqtrace_2W2R_allconf():
    """Build a trace of writes/reads that interleave 1/1 up to 4 total."""
    rq = []
    for i in range(2):
        rq.append(RPCRequest(2 * i, "theKey", True, predef_hash=0))
        rq.append(RPCRequest(2 * i + 1, "theKey", False, predef_hash=0))
    return rq


@pytest.fixture
def reqtrace_1W7R_allconf():
    """Build a trace of 1 blocking wr, with predef hashes all to the same bucket."""
    rq = [RPCRequest(0, "theKey", True, predef_hash=0)]
    for i in range(7):
        rq.append(RPCRequest(1 + i, "theKey", False, predef_hash=0))
    return rq


@pytest.fixture
def reqtrace_8R():
    return [RPCRequest(i, "theKey", False, predef_hash=0) for i in range(8)]


@pytest.fixture
def reqtrace_2R2W_diff():
    """Build a trace of 2 reads, and 2 following conflicting writes."""
    rq = []
    rq.append(RPCRequest(0, "theKey", False, predef_hash=0))
    rq.append(RPCRequest(1, "anotherKey", False, predef_hash=1))

    rq.append(RPCRequest(2, "theKey", True, predef_hash=0))
    rq.append(RPCRequest(3, "anotherKey", True, predef_hash=1))
    return rq


@pytest.fixture
def reqtrace_RW_50R():
    """Build a trace of read and write to the same bucket, followed by 50 reads to the
    same bucket. Useful to test that the write does not starve."""
    rq = []
    rq.append(RPCRequest(0, "theKey", False, predef_hash=0))  # rd 0
    rq.append(RPCRequest(1, "theKey", True, predef_hash=0))  # wr 0
    for i in range(50):
        rq.append(RPCRequest(2 + i, "theKey", False, predef_hash=0))  # rd 0
    return rq
