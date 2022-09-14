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
class DeferralController(object):
    """A class that implements a global deferral controller. Every N events, it will indicate to the next
    worker that it must excute a synchronization event."""

    def __init__(self, initial_value: int = 25) -> None:
        self.deferral_counter = initial_value
        self.reset_val = initial_value

    def check_defer(self) -> bool:
        """Return true if a worker must execute a synchronization event."""
        self.deferral_counter -= 1
        if self.deferral_counter == 0:
            self.deferral_counter = self.reset_val
            return True
        return False

    def reset_defer(self, counter_value: int = -1) -> None:
        """Reset the deferral counter to the initial value (optional arg for explicit value if provided)"""
        if counter_value != -1:
            self.deferral_counter = self.reset_val
        else:
            self.deferral_counter = counter_value

    def get_deferral_cost_multiplier(self) -> int:
        """Return a cost multiplier scaling with the number of deferred write events that happened before the current one."""
        return self.reset_val - self.deferral_counter
