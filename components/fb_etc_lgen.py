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

# Python packages
from scipy.optimize import least_squares
from tqdm import tqdm

from .comm_channel import CommChannel
from .zipf_gen import ZipfKeyGenerator
from .fb_etc_dists import ETCValueDistribution, ETCKeyDistribution
from .load_generator import OpenPoissonLoadGen, rollHit
from .requests import RPCRequest

import simpy


class ETCLoadGen(OpenPoissonLoadGen):
    """
    This class serves as an open-loop load generator whose generated RPC request objects
    approximate/conform to the ETCKey and ETCValueDistribution classes.
    """

    def __init__(
        self, simpy_env, out_queue, num_events, key_obj, incoming_load, writes
    ):
        """Initialize the superclass and this class' private objects."""
        super().__init__(
            simpy_env, out_queue, num_events, key_obj, incoming_load, writes
        )

        # Initialize etc distrs
        self.value_dist = ETCValueDistribution()
        self.key_dist = ETCKeyDistribution()

        # For all keys in the zipf distribution of "key_obj", associate them with a key length
        # and value length. Attempt to match the PDF of each distribution (e.g., if a particular
        # key has expected popularity of 0.1, its value should have expected popularity of 0.1).
        self.key_size_for_rank = [28 for i in range(key_obj.num_keys())]
        self.val_size_for_rank = [100 for i in range(key_obj.num_keys())]

        # print("*** Initializing key & value sizes from key probability distributions.")
        # TODO: Find a way to do this faster! Right now runs at ~30 sols/sec, meaning
        # this is intractable for simulations.
        """
        for k in tqdm(range(key_obj.num_keys())):
            prob_of_key = key_obj.prob_for_rank(k)

            # Match expected prob in value length distr
            def func_val_size(x):
                return prob_of_key - self.value_dist.pdf(x)
            def func_key_size(x):
                return prob_of_key - self.key_dist.pdf(x)

            #z = least_squares(func_val_size, 100, bounds=((0,1000000)))
            #z_key = least_squares(func_key_size, 8, bounds=((0,10000)))
            #self.val_size_for_rank[k] = z.x.astype(int)[0]
            #self.key_size_for_rank[k] = z_key.x.astype(int)[0]
        """

        print(
            "*** Finished initializing key & value sizes from key probability distributions."
        )

    def print_val_prob_mappings(self):
        for k in range(self.key_obj.num_keys()):
            print(
                "*** Rank",
                k,
                "prob = ",
                self.key_obj.prob_for_rank(k),
                "val size = ",
                self.val_size_for_rank[k],
            )

    def gen_new_req(self, rpc_id=-1):
        """Generate a new request. Override the superclass' method because we want to use the
        distributions from ETC rather than just a key and read/write."""

        # Setup request parameters
        is_write = rollHit(self.write_frac)
        rank = self.key_generator.get_rank()
        req = RPCRequest(
            rpc_id,
            self.key_generator.key_strings[rank],
            is_write,
            key_size=self.key_size_for_rank[rank],
            val_size=self.val_size_for_rank[rank],
        )
        req.generated_time = self.env.now
        return req

    # Functions useful when wanting to create this object once and then feed in
    # per-simulation parameters later on.
    def set_incoming_load(self, new_load: float):
        """Reset the incoming load arrival rate to parameter new_load."""
        self.myLambda = new_load

    def set_simpy_env(self, new_param: simpy.Environment):
        """Reset the simpy env to parameter provided."""
        self.env = new_param

    def set_out_queue(self, new_param: CommChannel):
        """Reset the simpy env to parameter provided."""
        self.env = new_param
