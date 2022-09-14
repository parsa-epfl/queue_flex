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
## Author: Mark Sutherland, (C) 2020
from simpy import Environment, Interrupt, Store
from .end_measure import EndOfMeasurements
from .requests import FuncRequestWithServTime, PullFeedbackRequest
from .load_generator import AbstractLoadGen

from random import randint
from csv import DictReader

## A class which serves as a closed-loop load generator for a microservice-simulation
## where all of the following are file-specified: function ids, interarrival times, serv. times
class ClosedLoopLoadGen(AbstractLoadGen):
    def __init__(self, simpy_env, out_queue, func_file):
        super().__init__()
        self.env = simpy_env
        self.ffile = func_file
        self.q = out_queue
        self.action = self.env.process(self.run())

    def set_core(self, core):
        # Will inform core of function dispatch if not None
        # for purposes of queue state tracking
        self.core_to_inform = core

    def gen_new_req(self, req_num, row):
        return FuncRequestWithServTime(req_num, row["f_type"], row["serv_time"])

    def next_interarrival(self, row):
        return row["int_time"]

    def run(self):
        numGenerated = 0
        row = {
            "f_type": 0,
            "serv_time": 100,
            "int_time": 200,
        }  # compatible w. DictReader API
        while numGenerated < 10:
            # Wait until the server sends a pull
            req_ack = yield self.q.get()
            assert isinstance(req_ack, PullFeedbackRequest) is True

            req = self.gen_new_req(numGenerated, row)
            req.dispatch_time = self.env.now

            # Now put a new request (the gen_new_req comes from the superclass)
            if self.core_to_inform is not None:
                self.core_to_inform.new_dispatch(req.getFuncType())

            yield self.q.put(req)
            numGenerated += 1
        # Make a new EndOfMeasurements event (special)
        yield self.q.put(EndOfMeasurements())


"""
    def run(self):
        numGenerated = 0
        with open(self.ffile,'r') as fh:
            f_reader = reader(fh) #TODO Convert to DictReader
            for row in f_reader:
                # Wait until the server sends a pull
                req_ack = yield out_queue.get()
                assert(isinstance(req_ack,PullFeedbackRequest) is True)

                # Now put a new request (the gen_new_req comes from the superclass)
                yield self.q.put(self.gen_new_req(numGenerated,row))
                yield self.env.timeout(self.next_interarrival(row))
                numGenerated += 1
"""
