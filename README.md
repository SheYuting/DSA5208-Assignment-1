# DSA5208 Distributed Database Consistency Project

## Overview

This project investigates **client-centric consistency** in a replicated Apache Cassandra database.

The four consistency models studied are:

* Read-your-writes consistency (RYW)
* Monotonic-reads consistency (MR)
* Monotonic-writes consistency (MW)
* Writes-follow-reads consistency (WFR)

The experiments compare several Cassandra consistency-level configurations under three operating scenarios:

1. Normal operation
2. Node failure
3. Network partition
4. Network Latency

The Cassandra cluster consists of three Docker containers with a replication factor of 3.

---

## Project Structure

```text
DSA5208-Assignment-1/
│
├── docker-compose.yml
├── consistency_suite_fixed.py
│
├── fail_cass3.ps1
├── recover_cass3.ps1
├── partition_cass3.ps1
├── heal_partition.ps1
├── check_cluster.ps1
├── inject_latency.ps1
├── fix_latency.ps1

│
├── results/
│   ├── normal.csv
│   ├── node_failure.csv
│   └── partition.csv
│   └── latency.csv


│
└── README.md
```

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

The experiment script creates a keyspace using `NetworkTopologyStrategy` with replication factor 3.

---

## Requirements

The experiments were designed for Windows with PowerShell.

Required software:

* Docker Desktop
* Python 3
* `uv`
* Cassandra Python Driver

Install the Python dependency with:

```powershell
uv pip install cassandra-driver
```

Alternatively:

```powershell
pip install cassandra-driver
```

Docker Desktop must have virtualization support enabled.

---

# 1. Starting the Cassandra Cluster

Start the cluster:

```powershell
docker compose up -d
```

Check container status:

```powershell
docker compose ps -a
```

All three containers should be running.

Example:

```text
cass1   Up
cass2   Up
cass3   Up
```

Cassandra may require some time after the containers start before CQL becomes available.

Check the cluster:

```powershell
docker exec cass1 nodetool status
```

A healthy three-node cluster should contain three `UN` entries:

```text
Datacenter: dc1

UN  <cass1-ip>
UN  <cass2-ip>
UN  <cass3-ip>
```

where:

```text
U = Up
N = Normal
```

The cluster can also be checked using:

```powershell
.\check_cluster.ps1
```

This verifies the Docker containers, Cassandra membership, and host CQL ports.

---

# 2. Consistency Configurations

The experiments test combinations of Cassandra write and read consistency levels.

The default configurations are:

```text
W=ONE,    R=ONE
W=ONE,    R=QUORUM
W=QUORUM, R=ONE
W=QUORUM, R=QUORUM
W=ALL,    R=ONE
```

For a replication factor of 3:

```text
ONE    = 1 replica response
QUORUM = 2 replica responses
ALL    = 3 replica responses
```

These configurations allow the experiment to compare weaker consistency settings with quorum-based settings and observe the trade-off between consistency and availability.

---

# 3. Client-Centric Consistency Experiments

The main experiment script is:

```text
consistency_suite_fixed.py
```

The experiment uses explicit versions and separates consistency violations from operation failures.

Possible outcomes are:

```text
PASS
BREACH
FAILED
INCONCLUSIVE
```

`FAILED` is kept separate from `BREACH` because an unavailable operation is not itself a consistency violation.

---

## 3.1 Read-Your-Writes

The logical operation order is:

```text
Client A:

WRITE x = version 1
        |
        v
write successfully returns
        |
        v
READ x from another coordinator
```

A violation occurs if the write succeeds but the subsequent read observes an older version.

---

## 3.2 Monotonic Reads

The experiment performs sequential reads:

```text
WRITE x = version 1

READ 1 -> version v1
READ 2 -> version v2
```

A violation occurs when:

```text
v2 < v1
```

For example:

```text
READ 1 -> version 1
READ 2 -> version 0
```

means that the client has observed time moving backwards.

---

## 3.3 Monotonic Writes

Two distinct items are used so that Cassandra's last-write-wins behavior on a single row does not confuse write ordering with conflict resolution.

The client performs:

```text
W1
 |
 v
W1 successfully returns
 |
 v
W2
```

The observer then checks whether the later write is visible while the earlier write is not.

Conceptually:

```text
Client A:

W1 -> W2

Observer:

W2 visible
W1 not visible
```

This is treated as a monotonic-writes violation.

---

## 3.4 Writes-Follow-Reads

An explicit causal dependency is created:

```text
Client A:
WRITE cause = 1

Client B:
READ cause = 1
      |
      v
WRITE effect = 1
```

This produces:

```text
WRITE(cause)
      |
      v
READ(cause)
      |
      v
WRITE(effect)
```

The observer then checks whether the dependent `effect` write is visible without the `cause` write.

A state such as:

```text
effect = 1
cause  = 0
```

is treated as a writes-follow-reads violation.

---

# 4. Normal Operation Experiment

Before running the experiment, verify that all three nodes are healthy:

```powershell
docker exec cass1 nodetool status
```

Then run:

```powershell
python consistency_suite_fixed.py --scenario normal --trials 100 --output results/normal.csv
```

or with `uv`:

```powershell
uv run python consistency_suite_fixed.py --scenario normal --trials 100 --output results/normal.csv
```

The script first checks that ports:

```text
9042
9043
9044
```

correspond to three distinct Cassandra host IDs.

It then seeds version-0 baseline records using `CL.ALL`.

Example output:

```text
Pinned coordinator endpoints:
  n1: 127.0.0.1:9042 host_id=...
  n2: 127.0.0.1:9043 host_id=...
  n3: 127.0.0.1:9044 host_id=...

Seeding baseline version=0 at CL.ALL ...
Baseline seeding complete on all replicas.
```

The experiment then executes all four consistency tests for each consistency-level configuration.

A lack of observed violations under normal operation should not be interpreted as proof that a weak consistency level guarantees the corresponding client-centric consistency property.

---

# 5. Node Failure Experiment

The node failure experiment simulates the complete failure of `cass3`.

First ensure that all three nodes are healthy.

Run:

```powershell
python consistency_suite_fixed.py --scenario node-failure --trials 30 --output results/node_failure.csv
```

The program first creates baseline data while all replicas are available.

It then pauses with a message similar to:

```text
BASELINE READY. Inject the node failure now.
```

Open another PowerShell terminal and execute:

```powershell
.\fail_cass3.ps1
```

This stops `cass3`.

Return to the Python terminal and press Enter to continue the experiment.

After the experiment, restore the node:

```powershell
.\recover_cass3.ps1
```

Check that the cluster has recovered:

```powershell
.\check_cluster.ps1
```

or:

```powershell
docker exec cass1 nodetool status
```

All three nodes should eventually return to `UN`.

### Expected Behaviour

With one node unavailable and replication factor 3:

```text
ONE
```

can normally continue with one available replica.

```text
QUORUM
```

requires two replica responses and can therefore continue while two nodes remain available.

```text
ALL
```

requires all three replicas and should become unavailable when one node is down.

This scenario is useful for studying the trade-off between consistency level and availability.

---

# 6. Network Partition Experiment

The network partition experiment differs from node failure.

During node failure:

```text
cass3 = unavailable
```

During network partition:

```text
Python client -> cass3       reachable

cass3 -> cass1               blocked
cass3 -> cass2               blocked
```

Therefore `cass3` remains alive and may still serve stale data while being unable to synchronize with the other replicas.

This is particularly useful for exposing client-centric consistency violations.

---

## Docker Requirement for Partition Injection

The `cass3` service must have the `NET_ADMIN` capability.

Add the following to `cass3` in `docker-compose.yml`:

```yaml
cap_add:
  - NET_ADMIN
```

Example:

```yaml
cass3:
  image: cassandra:4.1
  container_name: cass3
  hostname: cass3

  ports:
    - "9044:9042"

  environment:
    - CASSANDRA_CLUSTER_NAME=TestCluster
    - CASSANDRA_SEEDS=cass1
    - CASSANDRA_DC=dc1
    - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
    - MAX_HEAP_SIZE=512M
    - HEAP_NEWSIZE=100M

  cap_add:
    - NET_ADMIN

  networks:
    - cass-net
```

Recreate `cass3` after changing the Compose configuration:

```powershell
docker compose up -d --force-recreate cass3
```

Wait until the three-node Cassandra cluster is healthy again.

---

## Running the Partition Experiment

Run:

```powershell
python consistency_suite_fixed.py --scenario partition --trials 30 --output results/partition.csv
```

The script first seeds baseline data while the cluster is healthy.

When the following prompt appears:

```text
BASELINE READY. Inject the network partition now.
```

open another PowerShell terminal and run:

```powershell
.\partition_cass3.ps1
```

The script blocks communication between:

```text
cass3 <-> cass1
cass3 <-> cass2
```

while attempting to preserve:

```text
Windows host -> 127.0.0.1:9044 -> cass3
```

The helper script should report:

```text
Host -> cass3:9044 is still reachable.
Network partition injected.
```

Return to the Python experiment and press Enter.

After the experiment, heal the partition:

```powershell
.\heal_partition.ps1
```

Then verify recovery:

```powershell
.\check_cluster.ps1
```

---
# 7. Network Latency
 
The network latency experiment simulates real-world network degradation and asymmetrical node delay.

First ensure that all three nodes are healthy.

Run:

```powershell
python consistency_suite_fixed.py --scenario partition --trials 30 --output results/latency.csv
```

The program first creates baseline data while all replicas are available.

It then pauses with a message similar to:

```text
BASELINE READY. Inject the network partition now.
Press ENTER after the fault/partition is active...
```

Open another PowerShell terminal and execute:

```powershell
.\inject_latency.ps1
```

This applies a 500ms delay to cass3.

Return to the Python experiment and press Enter.

After the experiment, fix the latency by running:

```powershell
.\fix_latency.ps1
```

### Expected Behaviour

With one node unavailable and replication factor 3:

```text
ONE
```

suffers heavy consistency breaches.

```text
QUORUM
```

maintains strong consistency with zero latency impact, provided queries contact fast replicas.
```text
ALL
```

maintains strong consistency, but is bottlenecked by the slowest replica.

This scenario illustrates the fundamental trade-off between query latency and consistency in distributed databases.
---




---

# 8. Output

Each experiment produces a CSV file.

Recommended result files:

```text
results/
├── normal.csv
├── node_failure.csv
└── partition.csv
└── latency.csv
```

Important CSV fields include:

```text
scenario
run_id
model
config
trial
outcome
stage
detail
observed_1
observed_2
```

The four model names are:

```text
RYW = Read Your Writes
MR  = Monotonic Reads
MW  = Monotonic Writes
WFR = Writes Follow Reads
```

The outcomes are:

```text
PASS
BREACH
FAILED
INCONCLUSIVE
```

For example:

```text
scenario,model,config,outcome
normal,RYW,W=ONE,R=ONE,PASS
partition,RYW,W=ONE,R=ONE,BREACH
node-failure,RYW,W=ALL,R=ONE,FAILED
```

---

# 12. AI Usage

Generative AI tools were used to assist with:

* reviewing and correcting experimental code;
* improving the experimental design for the four client-centric consistency models;
* debugging Docker and Cassandra deployment issues;
* generating helper scripts for fault injection and recovery; and
* assisting with documentation and explanation.

All generated code and experimental procedures were reviewed and executed by the project group. Experimental results reported in the final submission should be based on actual runs of the submitted code.
