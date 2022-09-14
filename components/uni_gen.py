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

#!/usr/bin/python3
## Author: Mark Sutherland, (C) 2020
## A class which returns integer values (TODO: variable length strings)
## distributed according to a uniform distribution.
from random import seed, uniform
from math import ceil


class UniformKeyGenerator(object):
    def __init__(self, **kwargs):
        # args needed from higher level:
        #   (num_items) -> Number of items in the dataset
        req_args = ["num_items"]
        for k in req_args:
            if k not in kwargs.keys():
                raise ValueError(
                    "Required", k, "argument not specified in UniformGenerator init"
                )

        self.theConfig = {"N": kwargs["num_items"]}
        self.theNumKeys = int(self.theConfig["N"])
        rand_seed = "0xdeadbeef"
        print("Seeding random with", rand_seed)
        seed(rand_seed)
        print("Done!")

    def get_key(self):
        # Algorithm: Get a random number in the specified interval, return that key rank.
        r = ceil(uniform(0, self.theNumKeys))
        return r
