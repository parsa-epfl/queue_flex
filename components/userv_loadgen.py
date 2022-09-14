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
from numpy.random import exponential as exp_arrival
from simpy import Environment, Interrupt, Store
from .end_measure import EndOfMeasurements
from .requests import FuncRequest
from .load_generator import AbstractLoadGen

from random import randint

## A class which serves as a load generator for a microservice-simulation with multiple req types.
class uServLoadGen(AbstractLoadGen):
    def __init__(
        self, simpy_env, out_queue, num_events, interarrival_time, num_functions
    ):
        super().__init__()
        self.env = simpy_env
        self.q = out_queue
        self.num_events = num_events
        self.myLambda = interarrival_time
        self.num_functions = num_functions
        self.action = self.env.process(self.run())

    def gen_new_req(self, rpc_id=-1):
        # Setup parameters id and func_type
        f_type = randint(0, self.num_functions - 1)
        req = FuncRequest(rpc_id, f_type)
        req.generated_time = self.env.now
        return req

    def run(self):
        numGenerated = 0
        while numGenerated < self.num_events:
            try:
                yield self.q.put(self.gen_new_req(numGenerated))
                yield self.env.timeout(exp_arrival(self.myLambda))
                numGenerated += 1
            except Interrupt as i:
                return

        # Make a new EndOfMeasurements event (special)
        yield self.q.put(EndOfMeasurements())

        # Keep generating events for realistic measurement
        while True:
            try:
                yield self.q.put(self.gen_new_req(-1))
                yield self.env.timeout(exp_arrival(self.myLambda))
            except Interrupt as i:
                return
