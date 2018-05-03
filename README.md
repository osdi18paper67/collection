Benchmarking and Collection Scripts for CloudLab Nodes
===========
##### OSDI '18 Paper #67

This repository contains a list of benchmarks that we execute in order to 
generate a dataset for performance (statistical) analysis of CloudLab machines.

The structure of the repository is the following:

- `STREAM/`. John McCalpin's STREAM benchmark with some minor modifications. 
  See the `STREAM/README` file for more details.
- `membench/`. Memory tests originally implemented by Alex W. Reece whose goal
  is to achieve maximum peak throughput. See the `membench/README.md` file
  for more details.
- `orchestration.py`. The script containing logic to allocate nodes, deploy
  software, run tests and retrieve results.
- `run_benchmarks.sh`. The main script that is executed on each node.
