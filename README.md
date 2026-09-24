# DSA5208 Distributed Database Consistency Project

## Overview

This project investigates **client-centric consistency** in a replicated Apache Cassandra database.

The four consistency models studied are:

* **Read-Your-Writes (RYW)**
* **Monotonic Reads (MR)**
* **Monotonic Writes (MW)**
* **Writes-Follow-Reads (WFR)**

The experiments compare several Cassandra consistency-level configurations under four operating scenarios:

1. Normal operation
2. Node failure
3. Network partition
4. Network latency

The Cassandra cluster consists of three Docker containers with a replication factor of 3.

---

## Project Structure

```text
DSA5208-Assignment-1/
│
├── docker-compose.yml
├── consistency_suite_fixed.py
│
├── powershell_test/
│   ├── check_cluster.ps1
│   ├── fail_cass3.ps1
│   ├── recover_cass3.ps1
│   ├── partition_cass3.ps1
│   ├── heal_partition.ps1
│   ├── Inject-Latency.ps1
│   └── Fix-Latency.ps1
│
├── results/
│   ├── normal.csv
│   ├── node_failure.csv
│   ├── partition.csv
│   └── latency.csv
│
└── README.md
```

`consistency_suite_fixed.py` is the main experiment program.

The PowerShell scripts are used to check cluster health, inject failures, create network partitions, simulate network latency, and restore the cluster.

---

## System Architecture

The experiment uses three Cassandra 4.1 nodes:

```text
                Python Experiment Client
                  /        |        \
                 /         |         \
                v          v          v
             cass1       cass2       cass3
             :9042       :9043       :9044
                \          |          /
                 \         |         /
                  Cassandra Cluster
                 Replication Factor = 3
```

Host port mappings:

| Cassandra Node | Host Address     |
| -------------- | ---------------- |
| cass1          | `127.0.0.1:9042` |
| cass2          | `127.0.0.1:9043` |
| cass3          | `127.0.0.1:9044` |

The experiment script creates a Cassandra keyspace using `NetworkTopologyStrategy` with replication factor 3.

---

## Consistency Configurations

The following write/read consistency-level combinations are tested:

```text
W=ONE,    R=ONE
W=ONE,    R=QUORUM
W=QUORUM, R=ONE
W=QUORUM, R=QUORUM
W=ALL,    R=ONE
```

For replication factor 3:

```text
ONE    = 1 replica response
QUORUM = 2 replica responses
ALL    = 3 replica responses
```

The experiment records four possible outcomes:

```text
PASS
BREACH
FAILED
INCONCLUSIVE
```

A failed operation is recorded separately from a consistency breach.

---

# Setup

## Requirements

The experiments were developed on Windows using PowerShell.

Required software:

* Docker Desktop
* Python 3
* `uv`
* Cassandra Python Driver

Install the Python dependency with:

```powershell
uv pip install cassandra-driver
```

---

## Start the Cassandra Cluster

Start all Cassandra containers:

```powershell
docker compose up -d
```

Check the container status:

```powershell
docker compose ps -a
```

Cassandra may take some time to become ready after the containers start.

Check cluster membership:

```powershell
docker exec cass1 nodetool status
```

A healthy cluster should contain three `UN` nodes:

```text
UN  <cass1-ip>
UN  <cass2-ip>
UN  <cass3-ip>
```

where:

```text
U = Up
N = Normal
```

The helper script can also be used:

```powershell
.\powershell_test\check_cluster.ps1
```

All three Cassandra nodes should be healthy before starting an experiment.

---

# Running the Experiments

## 1. Normal Operation

Run:

```powershell
uv run python consistency_suite_fixed.py --scenario normal --trials 100 --output results/normal.csv
```

This runs all four client-centric consistency tests under normal cluster operation.

No fault or artificial network condition is introduced.

---

## 2. Node Failure

Start the experiment:

```powershell
uv run python consistency_suite_fixed.py --scenario node-failure --trials 30 --output results/node_failure.csv
```

The program first creates baseline data while all three replicas are available.

When the program pauses, open another PowerShell terminal and run:

```powershell
.\powershell_test\fail_cass3.ps1
```

This stops `cass3` and simulates a node failure.

Return to the Python terminal and press Enter to continue the experiment.

After the experiment, restore `cass3`:

```powershell
.\powershell_test\recover_cass3.ps1
```

Check that the cluster has recovered:

```powershell
.\powershell_test\check_cluster.ps1
```

With replication factor 3, `ONE` and `QUORUM` can continue operating while two nodes remain available, while operations using `ALL` are expected to lose availability when one replica is unavailable.

---

## 3. Network Partition

The network partition experiment keeps `cass3` running but blocks communication between `cass3` and the other Cassandra replicas.

For this experiment, `cass3` must have the Docker `NET_ADMIN` capability.

Ensure that `docker-compose.yml` contains:

```yaml
cap_add:
  - NET_ADMIN
```

for the `cass3` service.

Start the experiment:

```powershell
uv run python consistency_suite_fixed.py --scenario partition --trials 30 --output results/partition.csv
```

The program first creates baseline data while the cluster is healthy.

When it pauses, open another PowerShell terminal and run:

```powershell
.\powershell_test\partition_cass3.ps1
```

The intended topology is:

```text
cass1 <------> cass2

   X             X
    \           /
         cass3
```

`cass3` remains running and can still be reached by the Python client through port `9044`, but it cannot exchange Cassandra traffic with `cass1` and `cass2`.

Return to the Python terminal and press Enter.

After the experiment, restore network connectivity:

```powershell
.\powershell_test\heal_partition.ps1
```

Then verify cluster recovery:

```powershell
.\powershell_test\check_cluster.ps1
```

This scenario is intended to create replica divergence and expose differences between weak and stronger consistency configurations.

---

## 4. Network Latency

The network latency experiment simulates real-world network degradation and asymmetrical node delay.

First ensure that all three nodes are healthy.

Run:

```powershell
uv run python consistency_suite_fixed.py --scenario node-latency --trials 30
```

The results are saved to:

```text
results/latency.csv
```

The program first creates baseline data while all replicas are available.

When it pauses, open another PowerShell terminal and run:

```powershell
.\powershell_test\Inject-Latency.ps1
```

This applies approximately **500 ms delay to `cass3`**.

Return to the Python terminal and press Enter to continue the experiment.

After the experiment, remove the artificial latency:

```powershell
.\powershell_test\Fix-Latency.ps1
```

Then verify that the cluster is healthy:

```powershell
.\powershell_test\check_cluster.ps1
```

### Expected Behaviour

`ONE` may be more exposed to stale observations because only one replica response is required.

`QUORUM` can often continue using two responsive replicas and therefore may be less affected by a delayed third replica.

`ALL` requires responses from all replicas and is therefore expected to be most sensitive to the slowest replica.

This scenario illustrates the trade-off between **query latency, availability, and consistency** in a distributed database.

---

# Results

Experiment results are stored as CSV files in the `results/` directory.

Each record includes information such as:

```text
scenario
model
consistency configuration
trial
outcome
observed versions
```

The consistency-model abbreviations are:

```text
RYW = Read-Your-Writes
MR  = Monotonic Reads
MW  = Monotonic Writes
WFR = Writes-Follow-Reads
```

The expected final result files are:

```text
results/
├── normal.csv
├── node_failure.csv
├── partition.csv
└── latency.csv
```

---

## Normal Operation

The normal-operation experiment was run with:

```text
4 consistency models
× 5 read/write configurations
× 100 trials
= 2000 experiment records
```

All 2,000 recorded operations were:

```text
PASS
```

No consistency breaches, failures, or inconclusive outcomes were observed under normal operation.

This does not prove that every tested configuration guarantees all four client-centric consistency properties. In the healthy local Docker environment, replica communication is fast and the replicas normally remain synchronized.

---

## Node Failure

The node-failure experiment is expected to mainly show differences in **availability**.

With one replica unavailable:

* `ONE` can still operate with one available replica.
* `QUORUM` can still operate using the two surviving replicas.
* `ALL` cannot complete successfully because all three replicas are required.

Failures are therefore recorded separately from consistency breaches.

---

## Network Partition

The network-partition scenario is designed to create replica divergence while `cass3` remains available to the client.

Configurations using `ONE` may observe stale replica state because a read or write can complete using only one replica.

Quorum-based configurations require more replica agreement and may instead fail when sufficient replicas cannot communicate.

This scenario is therefore particularly useful for comparing consistency and availability.

---

## Network Latency

The network-latency scenario keeps all replicas available but makes `cass3` significantly slower.

The main expected effect is increased response latency, especially for consistency levels that must wait for more replicas.

`ALL` is particularly sensitive because the operation must wait for the slowest replica.

`QUORUM` may often complete using the two faster replicas, while `ONE` requires only one response.

The measured results are stored in:

```text
results/latency.csv
```

---

# Reproducing the Main Experiments

The recommended execution order is:

```text
1. Start Cassandra cluster
2. Verify all three nodes are UN
3. Run normal experiment
4. Run node-failure experiment
5. Recover cass3
6. Verify cluster health
7. Run network-partition experiment
8. Heal partition
9. Verify cluster health
10. Run network-latency experiment
11. Remove artificial latency
12. Verify final cluster health
```

---
