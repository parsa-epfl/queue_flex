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

from multiprocessing import Process, Queue
from queue import Empty

# from .core_dram_contention import simulateAppAndNI_DRAM

import importlib


def conv_to_string(k, v):
    return "--" + str(k) + " " + str(v)


def build_arg_string(d):
    elems = [conv_to_string(k, v) for (k, v) in d.items()]
    ret = ""
    for i in elems:
        ret += i
        ret += " "
    return ret


class SimpyInterface(Process):
    def __init__(
        self, simpy_arguments, t_id, q, result_q, num_jobs, rtarg, optional_arg_objects=None
    ):
        super().__init__()
        self.kwargs = dict(simpy_arguments)
        self.workQ = q
        self.resultQ = result_q
        self._njobs = num_jobs
        self.mode = self.kwargs["mode"]
        del self.kwargs["mode"]
        self.simpy_argstring = build_arg_string(self.kwargs)
        self.exp_to_run = rtarg
        self.optional_arg_objects = optional_arg_objects
        self.tid = t_id

    def run(self):
        jobs = []
        while len(jobs) < self._njobs:
            try:
                jobs.append(self.workQ.get(True, 10))
            except Empty:
                print("Could not get a job from workQ for 10 seconds, smth is wrong...")
                return

        # Import the module which is passed from parallel
        module_to_run = importlib.import_module("exps." + self.exp_to_run)

        jobs_completed = 0
        while len(jobs) > 0:
            strToPass = self.simpy_argstring
            job_id = jobs.pop()
            if "numservs" in self.mode:
                addMe = "-k " + str(job_id)
                strToPass += addMe
            elif "sweep_A" in self.mode:
                addMe = "-a " + str(job_id)
                strToPass += addMe
            elif "sweep_NI" in self.mode:
                # If sweep_NI, job_id is a RPC lambda, convert to explicit args
                nic_bw = (self.kwargs["rpcSizeBytes"] * 8.0) / (float(job_id))
                addMe = "--LambdaArrivalRate " + str(job_id)
                strToPass += addMe
                addMe = " --Bandwidth " + str(nic_bw)
                strToPass += addMe

            # Run it.
            if hasattr(module_to_run, "run_exp_w_optargs"):
                output = module_to_run.run_exp_w_optargs(
                    strToPass, self.optional_arg_objects
                )
            else:
                output = module_to_run.run_exp(strToPass)

            jobs_completed += 1
            print("*** Simulation thread {} finished task {} of {}....".format(self.tid,jobs_completed,self._njobs))
            self.workQ.task_done()
            self.resultQ.put_nowait({job_id: output})
