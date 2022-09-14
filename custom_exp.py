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

# Author: Mark Sutherland
# Purpose: This file shows the general workflow of customizing an experiment, by
# creating classes that consume events and then running an event simulation.
# Changelist
# - initial rev, sept. 2022

import argparse
import simpy
import typing
import random

SEED_MAGIC = 0xdeadbeef
NUM_WORKERS = 16
NUM_SUCCESSES = 4

class Mutex(object):
    """This class models a shared mutex, where only one worker can access it at a time."""

    def __init__(self,env):
        """Initialize the mutex."""
        self.env = env
        self.underlying_res = simpy.Resource(self.env) # Initialize the resource attached to the env

    def acquire(self):
        return self.underlying_res.request()

    def release(self, r):
        """Parameter r corresponds to the event which was returned by the call to mutex.acquire()"""
        return self.underlying_res.release(r)

def busy_work(env, mutex, work_id):
    """This function models a basic worker that sleeps for a random time, wakes up to take the mutex, and then
    releases it after a further random time. Each worker needs to get the mutex a given number of times before finishing."""

    num_successes = 0

    while num_successes < NUM_SUCCESSES:
        yield env.timeout(random.randint(1,10))
        print("*** Worker {} trying to acquire mutex at time {}".format(work_id,env.now))

        e = mutex.acquire() # Get an event "e" which will trigger when we can enter the critical section.
        yield e # If we acquired in line 57, this event returned is immediately triggered. if not, wait

        print("*** Worker {} in the critical section at time {}".format(work_id,env.now))
        yield env.timeout(random.randint(1,10))
        num_successes += 1
        mutex.release(e) # pass e back to the mutex object when releasing

        print("*** Worker {} released mutex at time {}".format(work_id,env.now))

def main():
    random.seed(SEED_MAGIC)

    # First, create the simpy environment which instantiates all the underlying event infra.
    env = simpy.Environment()

    # Create the shared mutex
    mutex = Mutex(env)

    # Create all the workers as simpy processes executing the function "busy_work". For instructions on how to
    # wrap these workers in an object, see the classes in the file "components/datastore_rpc.py".
    workers = [ env.process(busy_work(env, mutex, i)) for i in range(NUM_WORKERS) ]

    # Run the simulation until each worker gets the mutex the specified number of times.
    env.run()

if __name__ == "__main__":
    main()
