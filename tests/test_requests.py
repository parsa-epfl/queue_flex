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
from components.requests import RPCRequest

import pytest
import hashlib

rpc_num = 1
key_int = 1234
key_string = "theHashedString"


@pytest.fixture(name="req_nohash", scope="function", params=[key_int, key_string])
def create_req_nohash(request):
    return RPCRequest(rpc_num, request.param, False)


@pytest.fixture(name="req_predefhash", scope="function", params=[key_int, key_string])
def create_req_hash(request):
    predef_hash = calc_expected_hash(request.param)
    return RPCRequest(rpc_num, request.param, False, predef_hash)


def calc_expected_hash(k):
    h_obj = hashlib.sha256()
    h_obj.update(bytes(str(k).encode("utf-8")))
    return int(h_obj.hexdigest()[-16:-8], base=16)


def test_create_RPCRequest_nohash(req_nohash):
    assert req_nohash.getTotalServiceTime() == 0

    assert req_nohash.getWrite() is False
    req_nohash.setWrite()
    assert req_nohash.getWrite() is True

    gh = hash(req_nohash)
    assert gh == calc_expected_hash(req_nohash.key)


def test_create_RPCRequest_predefhash(req_predefhash):
    assert req_predefhash.getTotalServiceTime() == 0

    assert req_predefhash.getWrite() is False
    req_predefhash.setWrite()
    assert req_predefhash.getWrite() is True

    gh = hash(req_predefhash)
    assert gh == calc_expected_hash(req_predefhash.key)
