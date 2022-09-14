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
from ..components.global_sequencer import GlobalSequencer
import pytest


@pytest.mark.parametrize(
    "sequencer_init,expected_count", [pytest.param(None, 0), pytest.param(2, 2)]
)
def test_sequencer_init(sequencer_init, expected_count):
    if sequencer_init is None:
        s = GlobalSequencer()
    else:
        s = GlobalSequencer(sequencer_init)
    assert s.get_ts() == expected_count


@pytest.mark.parametrize(
    "inc_amount,expected_result", [pytest.param(None, 1), pytest.param(5, 5)]
)
def test_sequencer_increment(inc_amount, expected_result):
    s = GlobalSequencer()
    if inc_amount is None:
        s.increment_ts()
    else:
        s.increment_ts(inc_amount)
    assert s.get_ts() == expected_result
