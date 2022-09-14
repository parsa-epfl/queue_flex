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
# Author: Mark Sutherland, (C) 2021
from .requests import RPCRequest


def req_is_read(req: RPCRequest) -> bool:
    """Return true if the provided request is a read."""
    return not (req.getWrite())


def read_to_different_bucket(
    req: RPCRequest, bucket_match: int, num_buckets: int
) -> bool:
    """Return true if the provided req is a read to a different bucket."""
    req_hash = hash(req)
    if ((req_hash % num_buckets) != bucket_match) and req_is_read(req):
        return True
    return False


def younger_read_to_different_bucket(
    req: RPCRequest, bucket_match: int, ancestor_id: int, num_buckets: int
) -> bool:
    """Return true if the provided req is a read to a different bucket that is
    YOUNGER (smaller ID) than the id provided in ancestor_id.
    """
    if (
        read_to_different_bucket(req, bucket_match, num_buckets)
        and req.getID() < ancestor_id
    ):
        return True
    return False


def reqs_conflict(
    first_req: RPCRequest, second_req: RPCRequest, bucket_mod: int
) -> bool:
    """Return true if both requests can cause a conflict on the same index."""
    if first_req.getWrite() or second_req.getWrite():
        first_bucket = hash(first_req) % bucket_mod
        another_bucket = hash(second_req) % bucket_mod
        if first_bucket == another_bucket:
            return True

    return False
