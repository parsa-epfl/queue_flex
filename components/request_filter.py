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
# Author: Mark Sutherland, (C) 2021
from .requests import AbstractRequest
from typing import List


class RequestFilter:
    """
    A lambda class that returns True/False if the input AbstractRequest meets the
    condition passed to the \"holds\" function.
    """

    pass


class QueuedRequestAnalyzerInterface:
    """
    An interface class which contains methods that perform various analytical
    operations on the queued requests.
    """

    def get_reqs_dispatched_to_q(self, q_num: int) -> int:
        """Gets the number of requests dispatched to a private queue. Dispatched
        requests do not distinguish between queued and currently processing."""
        pass

    def get_reqs_in_private_q(self, q_num: int) -> int:
        """Gets the number of requests currently waiting in a private queue. Does NOT
        include currently processing."""
        pass

    def filter_reqs_in_private_q(
        self, q_num: int, f: RequestFilter
    ) -> List[AbstractRequest]:
        """Filters a private queue using a RequestFilter and return a list of the
        requests that match."""
        pass
