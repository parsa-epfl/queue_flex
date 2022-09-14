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
## Author: Mark Sutherland, (C) 2022

from components.load_generator import rollHit
from collections import Counter


class BimodalServTimeGenerator(object):
    """A generator class that returns bimodally distributed service times, according to a user-defined configuration.
    Mandatory parameters: the percentage of "short" requests, and the latency of each "short" and "long" request.
    """

    def __init__(
        self,
        short_percentage: float,
        short_lat: float,
        long_lat: float,
    ) -> None:
        self.type_counter = Counter(["long", "short"])
        self.short_percentage = short_percentage
        self.short_lat = short_lat
        self.long_lat = long_lat

    def get(self) -> float:
        short = rollHit(self.short_percentage)
        if short:
            self.type_counter["short"] += 1
            return self.short_lat
        else:
            self.type_counter["long"] += 1
            return self.long_lat

    def get_exp_stime(self):
        shorts = self.short_lat * float(self.short_perc) / 100
        longs = (self.long_lat) * (1 - (float(self.short_perc) / 100))
        return shorts + longs
