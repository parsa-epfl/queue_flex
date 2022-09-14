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

# Test the ETC load generator.
from ..components.fb_etc_lgen import ETCLoadGen
from ..components.fb_etc_dists import ETCValueDistribution
from ..components.zipf_gen import ZipfKeyGenerator
from .shared_fixtures import simpy_env_store, simpy_env

import pytest


@pytest.fixture(params=[(1000000, 0.99)])
def zipf_gen(request):
    num_keys = request.param[0]
    skew_coeff = request.param[1]
    return ZipfKeyGenerator(num_items=num_keys, coeff=skew_coeff)


@pytest.fixture
def etc_loadgen(zipf_gen, simpy_env_store):
    return ETCLoadGen(
        simpy_env=simpy_env_store["env"],
        out_queue=simpy_env_store["store"],
        num_events=100000,
        key_obj=zipf_gen,
        incoming_load=1,
        writes=1.0,
    )

    """
def test_lgen_creation(etc_loadgen):
    # Print the PDF to confirm it's similar to what's reported by FB's paper
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(1,1)
    ax.set_xscale("log")
    ax.set_yscale("log")
    x_dat = []
    y_dat = []
    for x in range(comp_under_test.key_generator.num_keys()):
        x_dat.append(comp_under_test.val_size_for_rank[x])
        y_dat.append(comp_under_test.key_generator.prob_for_rank(x))

    ax.scatter(x_dat,y_dat)
    plt.show()
    """


def test_lgen_makes_requests(etc_loadgen):
    for x in range(10):
        r = etc_loadgen.gen_new_req()
        print(r.get_val_size(), r.get_key_size())
