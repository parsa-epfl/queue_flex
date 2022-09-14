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
from components.comm_channel import CommChannel, portable_get_q_depth
from .shared_fixtures import simpy_env
import simpy


def test_channel_construct():
    env = simpy.Environment()
    cc = CommChannel(env, delay=10)
    assert cc.delay == 10
    assert cc.env is not None


def test_channel_get_put(simpy_env):
    expected_delay = 10
    cc = CommChannel(simpy_env, delay=expected_delay)

    def getter(env, cc, exp_item, time_log):
        yield env.timeout(expected_delay + 150)
        item = yield cc.get()
        assert item == exp_item
        time_log["get"] = env.now

    def putter(env, cc, item, time_log):
        cc.put(item)
        time_log["put"] = env.now
        yield env.timeout(expected_delay + 1)
        assert portable_get_q_depth(cc) == 1

    time_log = {}
    simpy_env.process(putter(simpy_env, cc, "iamanitem", time_log))
    simpy_env.process(getter(simpy_env, cc, "iamanitem", time_log))
    simpy_env.run()

    assert (time_log["get"] - time_log["put"]) == expected_delay + 150
