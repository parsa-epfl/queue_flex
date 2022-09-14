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

import simpy


def is_odd(val):
    if (val & 0x1) > 0:
        return True
    return False


class BucketedIndex(object):
    def __init__(self, num_buckets):
        self.num_buckets = num_buckets
        self.buckets = [0 for i in range(self.num_buckets)]
        self.event_waitlist = [list() for i in range(self.num_buckets)]

    def get_num_buckets(self):
        return self.num_buckets

    def get_index_version(self, bucket_num):
        return self.buckets[bucket_num]

    def set_index_version(self, bucket_num, new_version):
        self.buckets[bucket_num] = new_version

    def inc_index_version(self, bucket_num, increment=1):
        self.buckets[bucket_num] += increment
        return self.buckets[bucket_num]

    def get_event_for_increment(self, running_env, bucket_num):
        new_event = running_env.event()
        self.event_waitlist[bucket_num].append(new_event)
        return new_event

    def get_cb_event_increment(self, running_env, bucket_num, cb):
        new_event = running_env.event()
        new_event.callbacks.append(cb)
        self.event_waitlist[bucket_num].append(new_event)
        return new_event

    def succeed_event_for_bucket(self, bucket_num):
        for e in self.event_waitlist[bucket_num]:
            e.succeed((bucket_num, self.get_index_version(bucket_num)))
        self.event_waitlist[bucket_num].clear()


class AsyncIndexUpdater(object):
    """
    This class models a simple delay updater. It starts, yiels for a certain time, and then updates a particular counter
    inside a BucketedIndex.
    """

    def __init__(
        self,
        env: simpy.Environment,
        bucketed_index_obj: BucketedIndex,
        idx_to_update: int,
        delay: int,
    ) -> None:
        self.env = env
        self.bindex = bucketed_index_obj
        self.idx_to_update = idx_to_update
        self.delay = delay

        self.action = self.env.process(self.run())
        self.finished = False

    def run(self):
        """Function called by the simpy event loop."""
        yield self.env.timeout(self.delay)

        # print("ASYNC--- bucket index",self.idx_to_update,"incremented at time",self.env.now)
        # Increment the index counter and succeed any events waiting on it.
        self.bindex.inc_index_version(self.idx_to_update)
        self.bindex.succeed_event_for_bucket(self.idx_to_update)
        self.finished = True
