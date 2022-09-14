# Queue-Flex

Queue-Flex is an open-source research tool that is used for modelling the behaviour of various queueing systems. It is built on top of the discrete-event simulator `simpy`.

The main purpose of this tool is to provide a higher level of abstraction than CPU simluation to model things like load balancing, multi-threaded applications, and server components.

## Repository Structure

At the top level are all of the *.py files that can be used to launch a particular experiment, simulation, or generate a dataset.

The tools used by each top-level file are generally found in two directories:
- exps/ contains all the various experiments that can be run. For example, the file core_dram_bwcontention.py runs a simulation to study bandwidth interference between CPU cores and incoming NIC packets.
- components/ contains all the underlying components (e.g., a load balancer) that can be instantiated into a particular experiment.
- parallel/ contains a wrapper around python's multiprocessing tool to fork/join a large number of simulations at various load points
- interfaces/ contains a class which sits between "exps" and simpy to create simulations, environments, and run them to completion
- tests/ contains all the unit tests
- plot/ contains convenience scripts for plotting and displaying results
- util/ contains some common functionality shared across modules

## Usage

First, install everything in a virtual python environment. Queue-Flex only supports python3, with versions tested up to 3.9.7 at release time.
Then, run the tests and make sure everything works: `py.test`

## Experiments to Reproduce

The discrete-event simulations from the paper "The Nebula RPC-Optimized Architecture" can all be reproduced by using the top-level file "neb_qmodel.py".

For the paper "Cooperative Concurrency Control for Write-Intensive Key-Value Workloads", use the following top-level files and their corresponding experiments:
compare_system_excess_tlat.py -> Compares excess tail latency of various concurrency control policies.
crew_comp_sim.py -> Generates the throughput under SLO of the CREW policy as well as CREW plus compaction.
comp_detailed_study.py -> Allows studying the various statistics of d-CREW and compaction, plotting a user-defined graph per load point.

