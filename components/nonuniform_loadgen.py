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
from .requests import FuncRequest
from .userv_loadgen import uServLoadGen

from random import random
from bisect import bisect_right

## A class which serves as a load generator for a microservice-simulation, with non-uniform
## request types.
class NonUniformUServLoadGen(uServLoadGen):
    def conv_cumulative_vect(self, pvec):
        cumsum = 0
        ovec = []
        for x in pvec:
            cumsum += x
            ovec.append(cumsum)
        return ovec

    def __init__(
        self,
        simpy_env,
        out_queue,
        num_events,
        interarrival_time,
        num_functions,
        pop_vector,
    ):
        super().__init__(
            simpy_env, out_queue, num_events, interarrival_time, num_functions
        )
        assert len(pop_vector) == num_functions
        self.pop_vector = pop_vector
        self.cvec = self.conv_cumulative_vect(self.pop_vector)

    def gen_new_req(self, rpc_id=-1):
        # Setup parameters id and func_type
        r = random()  # standard interval

        f_idx = bisect_right(self.cvec, r)
        if f_idx < len(self.cvec):
            req = FuncRequest(rpc_id, f_idx)
            req.generated_time = self.env.now
            return req
        raise ValueError(
            "randint() generated",
            r,
            "and bisect_right returned idx",
            f_idx,
            "which is >= than the cdf array's length",
            len(self.cvec),
        )
