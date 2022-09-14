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
## A class which returns integer values distributed according to a parameterized zipf distribution.
from random import random
from bisect import bisect_right
from tqdm import tqdm

# from joblib import Parallel, delayed

import hashlib


class ZipfKeyGenerator(object):
    @staticmethod
    def calc_generalized_harmonic(n, power):
        harm = 0.0
        for i in tqdm(range(n), unit="flops"):
            harm += 1.0 / (pow(float(i + 1), power))
        return harm

    def num_keys(self):
        """Get the number of keys under this distribution."""
        return self.size

    def make_pdf_cdf_arrays(self):
        self.pdf_array = []
        self.cdf_array = []
        run_sum = 0.0
        for i in tqdm(range(self.size), unit="keys"):
            cur_rank_val = (1.0 / pow(i + 1, self.s)) / self.harmonic
            run_sum += cur_rank_val
            self.pdf_array.append(cur_rank_val)
            self.cdf_array.append(run_sum)

    def init_harmonics(self):
        self.harmonic = ZipfKeyGenerator.calc_generalized_harmonic(self.size, self.s)

    def hash_int_to_key(self, integer_rank):
        h_obj = hashlib.sha256()
        h_obj.update(bytes(str(integer_rank).encode("utf-8")))
        exp_keyhash = int(h_obj.hexdigest()[-16:-8], base=16)
        # return integer_rank, exp_keyhash
        return exp_keyhash

    def make_strings(self):
        self.key_strings = {}
        for i in tqdm(range(int(self.theConfig["N"])), unit="hashes"):
            self.key_strings[i] = self.hash_int_to_key(i)

    def __init__(self, **kwargs):
        """
        args needed from higher level:
            (num_items) -> Number of items in the dataset
            (coeff) -> Zipf coefficient
        """
        req_args = ["num_items", "coeff"]
        for k in req_args:
            if k not in kwargs.keys():
                raise ValueError(
                    "Required", k, "argument not specified in ZipfGenerator init"
                )

        self.theConfig = {"N": kwargs["num_items"], "s": kwargs["coeff"]}
        self.size = int(self.theConfig["N"])
        self.s = float(self.theConfig["s"])
        print(
            "*** Initializing generator with coefficient",
            self.theConfig["s"],
            "and",
            self.theConfig["N"],
            "keys...",
        )
        print("\tInitializing harmonic numbers....")
        self.init_harmonics()
        print("\tInitializing skewed PDF and CDFs....")
        self.make_pdf_cdf_arrays()
        print("\tMaking hashed keys....")
        self.make_strings()
        print("*** Done!")

    def hash_for_rank(self, k):
        return self.key_strings[k]

    def prob_for_rank(self, k):
        return self.pdf_array[k]

    def get_rank(self):
        """
        Return an item rank according to the initialized distribution.


        Algorithm: Get a random number in the standard interval
        Fit it into the cdf previously generated, and return the integer describing its rank.
        """
        r = random()
        rank = bisect_right(self.cdf_array, r)
        max_rank = len(self.cdf_array) - 1
        if rank < len(self.cdf_array):
            return rank
        elif rank == len(self.cdf_array):
            return max_rank
        else:
            raise ValueError(
                "rand() generated",
                r,
                "and bisect_right returned rank",
                rank,
                "which is >= than the cdf array's length",
                len(self.cdf_array),
            )

    def get_string_key(self):
        """
        Return a pre-hashed 8B string according to an item in the initialized distribution.


        Algorithm: Get a random number in the standard interval
        Fit it into the cdf previously generated, and get its rank. Then return the string of
        the item at that rank.
        """
        r = random()
        rank = bisect_right(self.cdf_array, r)
        if rank < len(self.cdf_array):
            return self.key_strings[rank]
        raise ValueError(
            "rand() generated",
            r,
            "and bisect_right returned rank",
            rank,
            "which is >= than the cdf array's length",
            len(self.cdf_array),
        )
