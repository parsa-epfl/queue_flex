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

from multiprocessing import Process, Queue, JoinableQueue, active_children
from math import floor

from interfaces.simpy_interface import SimpyInterface

import sys


class Invoker(object):
    def __init__(self, **kwargs):
        if "numProcs" not in kwargs.keys():
            raise ValueError("numProcs argument not specified in Invoker")
        else:
            self.numProcs = kwargs["numProcs"]
        if "runnableTarg" not in kwargs.keys():
            raise ValueError("runnableTarg argument not specified in Invoker")
        else:
            self.runTarg = kwargs["runnableTarg"]
        del kwargs["runnableTarg"]
        del kwargs["numProcs"]
        if "optargs" in kwargs.keys():
            self.opt_args = kwargs["optargs"]
            del kwargs["optargs"]
        else:
            self.opt_args = None

        argrange = kwargs["argrange"]

        self.input_queues = [JoinableQueue() for count in range(self.numProcs)]
        self.result_queues = [Queue() for count in range(self.numProcs)]

        # Divide up jobs and put into input_queues
        numJobs = len(argrange)
        jobsPerProc = floor(numJobs / self.numProcs)
        extraJobs = numJobs % self.numProcs

        curProc = 0
        maxProc = self.numProcs
        self.jobs_assigned = [0 for i in range(self.numProcs)]
        for job in argrange:
            if self.jobs_assigned[curProc] < jobsPerProc:
                self.input_queues[curProc].put_nowait(job)
                self.jobs_assigned[curProc] += 1
            else:
                curProc += 1
                if curProc >= maxProc:
                    curProc = 0
                self.input_queues[curProc].put_nowait(job)
                self.jobs_assigned[curProc] += 1

        del kwargs["argrange"]
        self.processes = [
            SimpyInterface(
                kwargs,
                count,
                self.input_queues[count],
                self.result_queues[count],
                self.jobs_assigned[count],
                self.runTarg,
                self.opt_args
            )
            for count in range(self.numProcs)
        ]

    def getResultsFromQueue(self, index):
        resultCount = 0
        results = []
        while resultCount < self.jobs_assigned[index]:
            results.append(self.result_queues[index].get())
            resultCount += 1
        return results

    def startProcs(self):
        for p in self.processes:
            p.start()
        # sys.excepthook = self.kill_active_procs

    def joinProcs(self):
        for q in self.input_queues:
            q.join()

    def kill_active_procs(self, exctype, value, traceback):
        """Hook for killing all processes if one of them receives an exception."""
        for p in active_children():
            p.terminate()
