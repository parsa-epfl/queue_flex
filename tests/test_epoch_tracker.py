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
from ..components.epoch_tracker import EpochTracker
from ..components.global_sequencer import GlobalSequencer
from .shared_fixtures import simpy_env

import pytest
import simpy

EPOCH_INIT = 1


@pytest.fixture
def sequencer():
    return GlobalSequencer(initial_value=EPOCH_INIT)


@pytest.fixture
def fixture_epoch_tracker(sequencer, simpy_env):
    e = EpochTracker(gs=sequencer, env=simpy_env)
    return {"EpochTracker": e, "GlobalSequencer": sequencer, "env": simpy_env}


def reader_reg_unreg(my_id, env, epoch_tracker, time_log):
    yield env.timeout(1)
    read_epoch = epoch_tracker.register_reader(my_id)
    assert read_epoch == EPOCH_INIT
    time_log["epoch_" + str(read_epoch) + "_register"] = env.now
    yield env.timeout(50)
    epoch_tracker.unregister_reader(read_epoch, my_id)
    time_log["epoch_" + str(read_epoch) + "_unreg"] = env.now


def writer(my_id, env, epoch_tracker, time_log):
    yield env.timeout(10)  # enough time for readers to register
    cur_epoch = epoch_tracker.get_cur_epoch()
    time_log["epoch_" + str(cur_epoch) + "_synch"] = env.now
    event, time_requested_synch = epoch_tracker.writer_synchronize_epoch(cur_epoch)
    yield event
    # Wakeup when readers have all unregistered
    assert cur_epoch not in epoch_tracker.waiting_writer_events
    assert cur_epoch not in epoch_tracker.epoch_to_reader_map
    time_log["epoch_" + str(cur_epoch) + "_expired"] = env.now


def test_readers_reg_unreg(fixture_epoch_tracker):
    e = fixture_epoch_tracker["EpochTracker"]
    s = fixture_epoch_tracker["GlobalSequencer"]
    env = fixture_epoch_tracker["env"]
    time_log = {}

    # Setup reader objects
    for i in range(2):
        env.process(reader_reg_unreg(i, env, e, time_log))

    env.run()


def test_writer_synch(fixture_epoch_tracker):
    e = fixture_epoch_tracker["EpochTracker"]
    s = fixture_epoch_tracker["GlobalSequencer"]
    env = fixture_epoch_tracker["env"]
    time_log = {}

    # Setup objects and run
    env.process(reader_reg_unreg(0, env, e, time_log))
    env.process(writer(1, env, e, time_log))
    env.run()
    assert time_log == {
        "epoch_1_register": 1,
        "epoch_1_unreg": 51,
        "epoch_1_synch": 10,
        "epoch_1_expired": 51,
    }


def test_writer_synch_noreaders(fixture_epoch_tracker):
    e = fixture_epoch_tracker["EpochTracker"]
    s = fixture_epoch_tracker["GlobalSequencer"]
    env = fixture_epoch_tracker["env"]
    time_log = {}

    env.process(writer(1, env, e, time_log))
    env.run()
    assert time_log == {
        "epoch_1_synch": 10,
        "epoch_1_expired": 10,
    }
