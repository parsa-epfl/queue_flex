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

from math import floor
from statistics import mean

from hdrh.histogram import HdrHistogram

import typing


class LatencyStoreWithBreakdown(object):
    def __init__(self, store_objects=True):
        self.store_objects = store_objects
        self.lat_store = HdrHistogram(1, 100000, 3)
        self.read_lat_store = HdrHistogram(1, 100000, 3)
        self.write_lat_store = HdrHistogram(1, 100000, 3)
        self.raw_req_objects = {}

    def record_value(self, req, lat):
        self.lat_store.record_value(lat)
        if req.getWrite():
            self.write_lat_store.record_value(lat)
        else:
            self.read_lat_store.record_value(lat)
        if self.store_objects:
            self.raw_req_objects[req.getID()] = req

    def get_req_at_percentile(self, perc, filter_reqs=False, is_write=False):
        """Return the request object which corresponds to the nth percentile of reads/writes, where perc is the percentile requested."""
        if not self.store_objects:
            return None
        if filter_reqs:
            if is_write:
                f = filter(lambda x: x.getWrite(), self.raw_req_objects.values())
            else:
                f = filter(lambda x: not (x.getWrite()), self.raw_req_objects.values())
        else:
            f = self.raw_req_objects.values()
        s = sorted(f, key=lambda item: item.getTotalServiceTime())
        if len(s) == 0:
            return None
        else:
            ordinal_num = floor(len(s) * (float(perc) / 100))
            return s[ordinal_num]

    def get_global_latency_percentile(self, perc):
        """Return the overall nth percentile latency of all requests added to this latency store."""
        return self.lat_store.get_value_at_percentile(perc)

    def get_filtered_latency_percentile(self, perc, reads=True):
        """Return the overall nth percentile latency of all reads/writes added to this latency store."""
        if reads:
            return self.read_lat_store.get_value_at_percentile(perc)
        else:
            return self.write_lat_store.get_value_at_percentile(perc)

    def get_latency_percentiles(
        self, perc_list: typing.List[float]
    ) -> typing.Dict[float, float]:
        """Return a dict of percentile values, indexed by the percentiles in perc_list argument."""
        return self.lat_store.get_percentile_to_value_dict(perc_list)

        """
        # Implementation that uses sorted raw objects. Histogram should be just as accurate.
        s = sorted(
            self.raw_req_objects.values(), key=lambda item: item.getTotalServiceTime()
        )
        if len(s) == 0:  # No objects/latencies recorded
            print(
                "WARNING: Requested the {}th percentile latency in LatencyStoreWithBreakdown, but no objects in the store! Returning 0.0. Suggest re-run or check data.".format(
                    nth_perc
                )
            )
            return 0.0
        else:
            ordinal_num = floor(len(s) * (float(perc) / 100))
            return s[ordinal_num].getTotalServiceTime()
        """

    def get_total_count(self):
        return self.lat_store.get_total_count()

    def get_read_objects(self):
        """Return the objects of all reads in sorted order."""
        if not self.store_objects:
            return None
        f = filter(lambda x: not (x.getWrite()), self.raw_req_objects.values())
        s = sorted(f, key=lambda item: item.getTotalServiceTime())
        return s

    def get_write_objects(self):
        """Return the objects of all writes in sorted order."""
        if not self.store_objects:
            return None
        f = filter(lambda x: x.getWrite(), self.raw_req_objects.values())
        s = sorted(f, key=lambda item: item.getTotalServiceTime())
        return s


class ExactLatStore(object):
    def __init__(self):
        self.latencies = []

    def record_value(self, lat):
        self.latencies.append(lat)

    def get_mean(self):
        return mean(self.latencies)

    def get_value_at_percentile(self, perc):
        s = sorted(self.latencies)
        if len(s):
            ordinal_num = floor(len(s) * (float(perc) / 100))
            return s[ordinal_num]
        else:
            return 0

    def add(self, s) -> None:
        self.latencies += s.latencies

    def get_iterable(self) -> typing.Iterable[typing.List]:
        return iter(self.latencies)
