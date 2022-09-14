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
from ..components.cache_state import KVPair, PrivateDataCache

import pytest


@pytest.fixture(params=[128])  # params = capacity
def cache_obj(request):
    return PrivateDataCache(request.param)


@pytest.fixture
def kvpairs_allfit():
    return [
        KVPair(key=i, value="unImportantValue", key_size=1, value_size=1)
        for i in range(64)
    ]


@pytest.fixture
def kvpairs_larger():
    return [
        KVPair(key=i, value="unImportantValue", key_size=1, value_size=1)
        for i in range(96)
    ]


@pytest.fixture
def kvpairs_multi_size(kvpairs_allfit):
    kvpairs_allfit.append(
        KVPair(key=134, value="unimportant", key_size=64, value_size=64)
    )
    return kvpairs_allfit


def test_cache_hits(cache_obj, kvpairs_allfit):
    for p in kvpairs_allfit:
        assert cache_obj.access(p) == (False, [])

    for p in kvpairs_allfit:
        assert cache_obj.peek(p) == True


def test_lru_replacement(cache_obj, kvpairs_larger):
    lru_key = 0
    for p in kvpairs_larger:
        if p.key() < 64:
            assert cache_obj.access(p) == (False, [])
        else:
            assert cache_obj.access(p) == (False, [kvpairs_larger[lru_key]])
            lru_key += 1


def test_multiple_replacement(cache_obj, kvpairs_multi_size):
    big_pair = kvpairs_multi_size.pop()
    for p in kvpairs_multi_size:
        assert cache_obj.access(p) == (False, [])

    hit, evicted_items = cache_obj.access(big_pair)
    assert hit == False
    assert evicted_items == kvpairs_multi_size
