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
## Author: Mark Sutherland, (C) 2020
import hashlib

# from cityhash import CityHash64

## A class that implements an abstract request type
class AbstractRequest(object):
    def __init__(self):
        self.generated_time = 0
        self.dispatch_time = 0
        self.start_proc_time = 0
        self.end_proc_time = 0
        self.completion_time = 0

    def getQueuedTime(self):
        return self.start_proc_time - self.generated_time

    def getProcessingTime(self):
        return self.end_proc_time - self.start_proc_time

    def getTotalServiceTime(self):
        return self.completion_time - self.generated_time

    def getPostProcessingTime(self):
        return self.completion_time - self.end_proc_time

    def getFuncType(self):
        return None

    def getID(self):
        return None


class NetworkedRPCRequest(AbstractRequest):
    def __init__(self, n, d, llc_hit):
        super().__init__()
        self.num = n
        self.generated_time = d
        self.dispatch_time = -1
        self.start_proc_time = -1
        self.end_proc_time = -1
        self.completion_time = -1
        self.hit = llc_hit

    def getNICTime(self):
        return self.dispatch_time - self.generated_time


## Class that models a request to one of a given number of functions
class FuncRequest(AbstractRequest):
    def __init__(self, rpc_number, f_type):
        super().__init__()
        self.num = rpc_number
        self.func_type = f_type
        self.has_affinity = False

    def getFuncID(self):
        return self.num

    def getFuncType(self):
        return self.func_type

    def affinityHit(self):
        return self.has_affinity

    def setAffinity(self):
        self.has_affinity = True

    def __str__(self):
        return "F" + str(self.func_type)


## Class that models a function request whose service time is pre-defined
class FuncRequestWithServTime(FuncRequest):
    def __init__(self, rpc_number, f_type, serv_time):
        super().__init__(rpc_number, f_type)
        self.serv_time = serv_time

    def getServiceTime(self):
        return self.serv_time


## A class that models an RPC request
class RPCRequest(AbstractRequest):
    def __init__(
        self, rpc_number, k, write, predef_hash=None, key_size=-1, val_size=-1
    ):
        super().__init__()
        self.num = rpc_number
        self.key = k
        self.isWrite = write
        self.key_size = key_size
        self.val_size = val_size
        self.num_cc_spins = 0
        self.num_cc_aborts = 0

        self.delayed_bool = False

        if predef_hash is not None:
            self.h = predef_hash
        else:
            h_obj = hashlib.sha256()
            h_obj.update(bytes(str(self.key).encode("utf-8")))
            self.h = int(h_obj.hexdigest()[-16:-8], base=16)
            # self.h = CityHash64(str(self.key))

    def __hash__(self):
        return self.h

    def getWrite(self):
        return self.isWrite

    def setWrite(self):
        self.isWrite = True

    def getID(self):
        return self.num

    def get_key_size(self):
        return self.key_size

    def get_val_size(self):
        return self.val_size

    def set_key_size(self, s):
        self.key_size = s

    def set_val_size(self, s):
        self.val_size = s

    def was_delayed(self) -> bool:
        return self.delayed_bool

    def set_delayed(self, d: bool) -> None:
        self.delayed_bool = d

    def __str__(self):
        """Return a string representing this object."""
        qtime = self.getQueuedTime()
        proctime = self.getProcessingTime()
        postproc = self.getPostProcessingTime()
        total = self.getTotalServiceTime()
        return f"\nRPCRequest (id: {self.num}), isWrite = {self.isWrite}, key = {self.key}, hash = {self.h}\n\tQ Time: {qtime}\n\tProc. Time: {proctime}\n\tPost Proc Time: {postproc}\n\tTotal Serv Time: {total}"


## A class that models an rpc processor asking the load balancer for a new request
class PullFeedbackRequest(AbstractRequest):
    def __init__(self, proc_num, req_completed=None):
        super().__init__()
        self.proc_id = proc_num
        self.req_completed = req_completed

    def getID(self):
        return self.proc_id

    def hasCompletedReq(self):
        if self.req_completed is not None:
            return True
        else:
            return False

    def getCompletedReqType(self):
        return self.req_completed.getFuncType()
