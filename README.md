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

The configurations to reproduce experiments from the ``Cooperative Concurrency Control'' paper are found in the artifact appendix of the full paper.

