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
from components.cache_state import FunctionMissModel


def test_dispatch_history():
    c = FunctionMissModel(2 * 1024, func_worksets=None, hist_length=2)
    for x in range(4):
        c.dispatch(x)
        assert c.incoming_funcs[-1] == x
        assert len(c.incoming_funcs) <= c.hist_length
        c.move_from_inc_to_history()
        assert len(c.incoming_funcs) == 0
        assert len(c.func_history) <= c.hist_length
        assert c.func_history[-1] == x


def test_miss_estimation():
    # definitions
    NUM_FUNCTIONS = 4
    # workset
    w_set = {}
    # Generate a dict with each func having a list of addresses
    addr_base = 0xABCD0000
    for i in range(NUM_FUNCTIONS):
        w = []
        # 256-entry working set
        for j in range(256):
            w.append(addr_base)
            addr_base += 4
        addr_base -= 128 * 4
        w_set[i] = w

    c = FunctionMissModel(1024, w_set, hist_length=2)
    for x in range(2):
        c.dispatch(x)
    # Now, queue state is [ 1 0 ] -> C -> [ ]

    assert c.get_misses_for_function(1) == 0
    assert c.get_misses_for_function(0) == 8

    # Move 0 to the history queue and check the same thing is true
    c.move_from_inc_to_history()

    assert c.get_misses_for_function(1) == 0
    assert c.get_misses_for_function(0) == 8

    c.dispatch(2)
    # Now, queue state is [ 2 1 ] -> C -> [ 0 ]
    assert c.get_misses_for_function(2) == 0
    assert c.get_misses_for_function(1) == 8
    assert c.get_misses_for_function(0) == 16
