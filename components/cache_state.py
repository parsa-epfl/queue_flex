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
import collections


def container_to_cache_size(container):
    return len(container) * 4


class KVPair:
    """
    This class is a simple wrapper for a key/value pair. It is used by the class
    PrivateDataCache to model data stored in a private cache.
    """

    def __init__(self, key, value, key_size, value_size):
        self.key_obj = key
        self.value_obj = value
        self.key_size = key_size
        self.value_size = value_size

    def key(self):
        """Return the key of this KV pair."""
        return self.key_obj

    def value(self):
        """Return the value of this KV pair."""
        return self.value_obj

    def total_size(self):
        """Return the cumulative size of the key and value."""
        return self.key_size + self.value_size

    def __eq__(self, obj):
        retval = isinstance(obj, KVPair) and (
            obj.key() == self.key_obj
            and obj.value_obj() == self.value_obj
            and obj.total_size == self.total_size()
        )
        print("Got here, comparing", obj.key(), "with", self.key(), "returning", retval)
        return retval


class PrivateDataCache:
    """
    This class implements a basic model of a private data cache, that only takes into
    account storage of key-value pairs. Each KV Pair must have a specified size so that
    to model cache capacity misses. The cache uses LRU replacement.
    """

    def __init__(self, cache_size: int):
        """Initialize the cache model and its total size."""
        self.cache_size = cache_size
        self.cache = collections.OrderedDict()
        self.cur_size = 0

    def peek(self, kv_pair: KVPair):
        """Lookup the cache to see if it contains kv_pair. Return true or false."""
        if kv_pair.key() in self.cache:
            self.cache.move_to_end(kv_pair.key())
            return True
        return False

    def access(self, kv_pair: KVPair):
        """First lookup the the cache to see if it contains kv_pair. If not, replace
        the LRU block in the cache model.
        Returns: hit/miss, list of replaced KVPairs (empty if hit or no replacement)
        """
        was_hit = self.peek(kv_pair)
        if was_hit:
            return True, []
        else:
            self.cache[kv_pair.key()] = kv_pair
            self.cur_size += kv_pair.total_size()

            ev_list = []
            while self.cur_size > self.cache_size:
                old_key, evicted = self.cache.popitem(last=False)
                self.cur_size -= evicted.total_size()
                ev_list.append(evicted)
            return False, ev_list


class FunctionMissModel(object):
    """
    This class implements a basic model of an L1 instruction cache, whose state depends
    on the history of functions that have executed on a hypothetical core, connected to
    this cache. Each function brings in its own working set, and therefore:
        with the history of F1, F2, F3, F4, the cache's state will be given by
        the union of all functions coming before the current point in time.
    """

    def __init__(self, cache_size, func_worksets, hist_length):
        self.cache_size = cache_size  # in B
        self.worksets = func_worksets  # dictionary of {f_id : workset} (workset is a list of vaddrs)
        self.hist_length = hist_length
        self.incoming_funcs = collections.deque(maxlen=self.hist_length)
        self.func_history = collections.deque(maxlen=self.hist_length)

    def dispatch(self, func_id):
        self.incoming_funcs.append(func_id)
        assert len(self.incoming_funcs) <= self.hist_length

    def func_executed(self, func_id):
        self.func_history.append(func_id)
        if len(self.func_history) > self.hist_length:
            self.func_history.popleft()

    def move_from_inc_to_history(self):
        self.func_history.append(self.incoming_funcs.popleft())
        if len(self.func_history) > self.hist_length:
            self.func_history.popleft()

    # Calculate the union of worksets and overlaps in reverse order starting from the tail of the dispatch q
    def add_to_cache_state(self, cache_state, f_list):
        # Base case: Nothing left in the list of incoming functions OR aggregate size is >= cache size
        if len(f_list) == 0:
            return cache_state
        else:
            f_inc = f_list.pop()
            if (
                container_to_cache_size(cache_state)
                + container_to_cache_size(self.worksets[f_inc])
                >= self.cache_size
            ):
                size_left = self.cache_size - container_to_cache_size(cache_state)
                idx = 0
                while size_left > 0 and idx < len(
                    self.worksets[f_inc]
                ):  # add a new element from the new workset repeatedly until filled
                    state_before = len(cache_state)
                    cache_state.add(self.worksets[f_inc][idx])
                    state_after = len(cache_state)
                    if state_before != state_after:
                        size_left -= 4
                    idx += 1
            else:
                # Recursive case: Calculate the union of f_inc with the previous state
                cache_state = set(self.worksets[f_inc]).union(
                    self.add_to_cache_state(cache_state, f_list)
                )
            return cache_state

    def compute_cache_state(self, is_being_executed):
        cache_state = set()
        if is_being_executed:
            self.incoming_funcs.popleft()

        # Account for functions in reverse from the incoming function queue.
        inc_copy = self.incoming_funcs.copy()
        cache_state = self.add_to_cache_state(cache_state, inc_copy)

        if (len(cache_state) * 4) < self.cache_size:
            # Add functions from the outgoing function queue if needed.
            hist_copy = self.func_history.copy()
            cache_state = self.add_to_cache_state(cache_state, hist_copy)

        return cache_state

    def get_misses_for_function(self, incoming_func_id, is_being_executed=False):
        # Given a new function, need to compute the pseudo-cache state before determining
        # how many misses it can be expected to incur
        cstate = self.compute_cache_state(is_being_executed)

        # Compute expected misses - set difference of this working set vs cache state ( turn into cb addrs )
        workset_missing = set(self.worksets[incoming_func_id]).difference(cstate)
        cbaddrs = set(map(lambda x: x >> 6, workset_missing))
        return len(cbaddrs)  # number of unique cbaddrs
