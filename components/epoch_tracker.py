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
from .global_sequencer import GlobalSequencer
import simpy


class EpochTracker(object):
    """A class that implements an epoch tracker for use in modelling concurrent systems implementing RLU/RCU.
    Processes can use this class to register/unregister themselves as readers/writers for a certain epoch, and request
    notifications when epochs end.
    """

    def __init__(self, gs: GlobalSequencer, env: simpy.Environment) -> None:
        """Initialize with a global sequencer object that gives out epoch numbers."""
        self.ts_object = gs
        self.env = env  # simpy environment used for quiescent period events

        # Data structures:
        #   - for each epoch, create a list of readers that are holding refs to objects of interest
        self.epoch_to_reader_map = {}

        #   - for each epoch, a writer can be waiting for its end, by yielding an event
        self.waiting_writer_events = {}

    def get_cur_epoch(self) -> int:
        """Return the current epoch number."""
        return self.ts_object.get_ts()

    def register_reader(self, reader_id: int) -> int:
        """Register a new reader object identified by reader_id. Returns the current epoch number."""
        cur_epoch = self.ts_object.get_ts()
        if cur_epoch in self.epoch_to_reader_map:
            self.epoch_to_reader_map[cur_epoch].append(reader_id)
        else:
            self.epoch_to_reader_map[cur_epoch] = [reader_id]

        return cur_epoch

    def unregister_reader(self, epoch_number: int, reader_id: int) -> None:
        """Unregister the reader identified by reader_id from the epoch given by epoch_number. No return."""
        assert (
            epoch_number in self.epoch_to_reader_map
        ), "Reader id {} tried to signal a quiescent state, but epoch number {} was NOT being tracked! Double-unregister??".format(
            reader_id, epoch_number
        )
        reader_list = self.epoch_to_reader_map[epoch_number]
        assert (
            reader_id in reader_list
        ), "Reader id {} tried to signal quiescent state, but it was NOT found on the reader list for this epoch! Reader list = {}".format(
            reader_id, reader_list
        )
        reader_list.remove(reader_id)

        if (
            len(reader_list) == 0
        ):  # we reached a quiescent period, trigger any writers waiting for it to end
            del self.epoch_to_reader_map[epoch_number]
            if epoch_number in self.waiting_writer_events:
                for e in self.waiting_writer_events[epoch_number]:
                    e.succeed()
                del self.waiting_writer_events[epoch_number]

    def writer_synchronize_epoch(self, epoch_number: int) -> simpy.Event:
        """Return a new event for the current epoch, which will be triggered after all readers unregister themselves.
        This can be used by writer processes to wait for a quiescent period."""
        qp_ending_event = simpy.Event(self.env)
        if epoch_number in self.epoch_to_reader_map:
            if epoch_number in self.waiting_writer_events:
                self.waiting_writer_events[epoch_number].append(qp_ending_event)
            else:
                self.waiting_writer_events[epoch_number] = [qp_ending_event]
        else:  # no readers had yet registered themselves for this epoch, equivalent to it being already done.
            qp_ending_event.succeed()
        return qp_ending_event, self.env.now

    def num_readers_registered(self, epoch_number: int) -> int:
        """Return how many readers are currently registered for a given epoch"""
        if epoch_number in self.epoch_to_reader_map:
            return len(self.epoch_to_reader_map[epoch_number])
        else:
            return 0
