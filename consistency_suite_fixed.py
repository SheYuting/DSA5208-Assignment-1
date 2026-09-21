#!/usr/bin/env python3
"""
Corrected Cassandra client-centric consistency experiment suite.

Designed for the user's 3-node Docker setup:
    cass1 -> 127.0.0.1:9042
    cass2 -> 127.0.0.1:9043
    cass3 -> 127.0.0.1:9044

Main corrections compared with the original prototype:
1. Read-your-writes is strictly WRITE-success -> READ.
2. Monotonic reads checks version regression across two sequential reads.
3. Monotonic writes uses two distinct items and enforces W1-success -> W2.
4. Writes-follow-reads uses an explicit causal dependency:
      write(cause) -> read(cause) -> write(effect)
   and checks whether effect is visible where cause is not.
5. Operational failures are reported separately from consistency breaches.
6. Each NodeClient is pinned to one contact endpoint using a whitelist
   execution profile and a fall-through retry policy.
7. Baselines are seeded at CL.ALL before any injected fault.
8. Results are saved as CSV for report/reproducibility.

Install:
    pip install cassandra-driver

Examples:
    python consistency_suite_fixed.py --scenario normal
    python consistency_suite_fixed.py --scenario node-failure
    python consistency_suite_fixed.py --scenario partition

For node-failure / partition scenarios, this script seeds all baseline rows while
the cluster is healthy, then pauses and asks you to inject the fault. Apply the
fault only after the prompt appears.
"""

import argparse
import csv
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path

from cassandra import ConsistencyLevel
from cassandra.cluster import (
    Cluster,
    ExecutionProfile,
    EXEC_PROFILE_DEFAULT,
)
from cassandra.policies import (
    WhiteListRoundRobinPolicy,
    FallthroughRetryPolicy,
)
from cassandra.query import SimpleStatement


NODE_PORTS = {
    "n1": 9042,
    "n2": 9043,
    "n3": 9044,
}

CL_MAP = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
}

DEFAULT_CONFIGS = [
    ("ONE", "ONE"),
    ("ONE", "QUORUM"),
    ("QUORUM", "ONE"),
    ("QUORUM", "QUORUM"),
    ("ALL", "ONE"),
]

MODELS = ("RYW", "MR", "MW", "WFR")


class NodeClient:
    """
    One client connection pinned to one host-side Cassandra endpoint.

    We intentionally use a whitelist policy for this experiment so that
    queries sent through n1/n2/n3 do not round-robin to another discovered
    Cassandra node.
    """

    def __init__(self, name: str, port: int, request_timeout: float = 4.0):
        self.name = name
        self.port = port

        profile = ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
            retry_policy=FallthroughRetryPolicy(),
            request_timeout=request_timeout,
        )

        self.cluster = Cluster(
            contact_points=["127.0.0.1"],
            port=port,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        )
        self.session = self.cluster.connect()

    def set_keyspace(self, keyspace: str):
        self.session.set_keyspace(keyspace)

    def identity(self):
        row = self.session.execute(
            "SELECT host_id, cluster_name, data_center, rack "
            "FROM system.local"
        ).one()
        return row

    def shutdown(self):
        self.cluster.shutdown()


def cl_name(cl: int) -> str:
    return ConsistencyLevel.value_to_name.get(cl, str(cl))


def parse_configs(raw: str):
    configs = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            w_name, r_name = [x.strip().upper() for x in part.split(":", 1)]
        except ValueError as exc:
            raise ValueError(
                f"Invalid config '{part}'. Use e.g. ONE:ONE,QUORUM:QUORUM"
            ) from exc

        if w_name not in CL_MAP or r_name not in CL_MAP:
            raise ValueError(
                f"Unsupported config '{part}'. Allowed levels: "
                f"{', '.join(CL_MAP)}"
            )
        configs.append((w_name, r_name))

    if not configs:
        raise ValueError("At least one consistency config is required.")
    return configs


def setup_schema(nodes):
    """
    Create schema while all three nodes are healthy.
    NetworkTopologyStrategy matches the single dc1 deployment.
    """
    s = nodes["n1"].session

    s.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS demo
        WITH replication = {
            'class': 'NetworkTopologyStrategy',
            'dc1': 3
        }
        """
    )

    # Wait for schema propagation to the three independently pinned sessions.
    deadline = time.time() + 30
    last_error = None

    while time.time() < deadline:
        try:
            for node in nodes.values():
                node.set_keyspace("demo")
            break
        except Exception as exc:
            last_error = exc
            time.sleep(0.5)
    else:
        raise RuntimeError(
            f"Keyspace 'demo' did not become visible on all nodes: {last_error}"
        )

    s.execute(
        """
        CREATE TABLE IF NOT EXISTS demo.consistency_events (
            run_id text,
            model text,
            config text,
            trial int,
            item text,
            version int,
            payload text,
            writer text,
            PRIMARY KEY ((run_id, model, config, trial), item)
        )
        """
    )

    deadline = time.time() + 30
    last_error = None
    while time.time() < deadline:
        try:
            for node in nodes.values():
                row = node.session.execute(
                    """
                    SELECT table_name
                    FROM system_schema.tables
                    WHERE keyspace_name='demo'
                    """
                )
                names = {r.table_name for r in row}
                if "consistency_events" not in names:
                    raise RuntimeError(
                        f"Table not visible yet on {node.name}"
                    )
            return
        except Exception as exc:
            last_error = exc
            time.sleep(0.5)

    raise RuntimeError(
        f"Table 'consistency_events' did not become visible on all nodes: "
        f"{last_error}"
    )


def write_item(
    node,
    run_id,
    model,
    config_name,
    trial,
    item,
    version,
    payload,
    writer,
    consistency,
):
    stmt = SimpleStatement(
        """
        INSERT INTO demo.consistency_events
            (run_id, model, config, trial, item, version, payload, writer)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        consistency_level=consistency,
    )

    try:
        node.session.execute(
            stmt,
            (
                run_id,
                model,
                config_name,
                trial,
                item,
                version,
                payload,
                writer,
            ),
        )
        return True, None
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def read_item(
    node,
    run_id,
    model,
    config_name,
    trial,
    item,
    consistency,
):
    stmt = SimpleStatement(
        """
        SELECT version, payload, writer
        FROM demo.consistency_events
        WHERE run_id=%s AND model=%s AND config=%s
          AND trial=%s AND item=%s
        """,
        consistency_level=consistency,
    )

    try:
        row = node.session.execute(
            stmt,
            (run_id, model, config_name, trial, item),
        ).one()
        return True, row, None
    except Exception as exc:
        return False, None, f"{type(exc).__name__}: {exc}"


def seed_baselines(nodes, run_id, configs, trials):
    """
    Seed every row at CL.ALL BEFORE a failure/partition is injected.

    This gives every replica a known version 0, making later stale reads
    explicit (version 0 vs version 1), rather than relying on missing rows.
    """
    seed_node = nodes["n1"]

    items_by_model = {
        "RYW": ["x"],
        "MR": ["x"],
        "MW": ["w1", "w2"],
        "WFR": ["cause", "effect"],
    }

    print("\nSeeding baseline version=0 at CL.ALL ...")

    for w_name, r_name in configs:
        config_name = f"W={w_name},R={r_name}"

        for model, items in items_by_model.items():
            for trial in range(trials):
                for item in items:
                    ok, err = write_item(
                        seed_node,
                        run_id,
                        model,
                        config_name,
                        trial,
                        item,
                        version=0,
                        payload="baseline",
                        writer="seed",
                        consistency=ConsistencyLevel.ALL,
                    )
                    if not ok:
                        raise RuntimeError(
                            "Baseline seeding failed. Faults must be injected "
                            f"only AFTER baseline seeding. "
                            f"{model}/{config_name}/trial={trial}/item={item}: "
                            f"{err}"
                        )

    print("Baseline seeding complete on all replicas.")


def result_row(
    scenario,
    run_id,
    model,
    config_name,
    trial,
    outcome,
    stage,
    detail="",
    observed_1="",
    observed_2="",
):
    return {
        "scenario": scenario,
        "run_id": run_id,
        "model": model,
        "config": config_name,
        "trial": trial,
        "outcome": outcome,
        "stage": stage,
        "detail": detail,
        "observed_1": observed_1,
        "observed_2": observed_2,
    }


def version_of(row):
    return None if row is None else row.version


def test_ryw(
    nodes,
    scenario,
    run_id,
    config_name,
    write_cl,
    read_cl,
    trials,
    remote_name,
):
    """
    Read-your-writes:
        same logical client: W(x=1) completes -> R(x)

    The read deliberately switches coordinator to remote_name.
    A breach is counted only if the write was acknowledged successfully.
    """
    out = []
    writer_node = nodes["n1"]
    reader_node = nodes[remote_name]

    for trial in range(trials):
        ok, err = write_item(
            writer_node,
            run_id,
            "RYW",
            config_name,
            trial,
            "x",
            1,
            "ryw-v1",
            "client-A",
            write_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "RYW", config_name, trial,
                    "FAILED", "write", err
                )
            )
            continue

        ok, row, err = read_item(
            reader_node,
            run_id,
            "RYW",
            config_name,
            trial,
            "x",
            read_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "RYW", config_name, trial,
                    "FAILED", "read", err
                )
            )
            continue

        observed = version_of(row)
        outcome = "PASS" if observed is not None and observed >= 1 else "BREACH"
        out.append(
            result_row(
                scenario, run_id, "RYW", config_name, trial,
                outcome,
                "check",
                f"write via n1, read via {remote_name}",
                observed_1=observed,
            )
        )

    return out


def test_monotonic_reads(
    nodes,
    scenario,
    run_id,
    config_name,
    write_cl,
    read_cl,
    trials,
    remote_name,
):
    """
    Monotonic reads:
        W(x=1)
        R1(x) -> observed version v1
        R2(x) -> observed version v2
        violation iff v2 < v1

    R1 is issued through n1 and R2 through a different coordinator.
    """
    out = []

    for trial in range(trials):
        ok, err = write_item(
            nodes["n1"],
            run_id,
            "MR",
            config_name,
            trial,
            "x",
            1,
            "mr-v1",
            "writer",
            write_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "MR", config_name, trial,
                    "FAILED", "write", err
                )
            )
            continue

        ok, row1, err = read_item(
            nodes["n1"],
            run_id,
            "MR",
            config_name,
            trial,
            "x",
            read_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "MR", config_name, trial,
                    "FAILED", "read1", err
                )
            )
            continue

        v1 = version_of(row1)

        # A monotonic-read violation requires the client to have first
        # observed a newer version. If R1 is stale, the precondition was not
        # established, so do not mislabel it as a breach.
        if v1 is None or v1 < 1:
            out.append(
                result_row(
                    scenario, run_id, "MR", config_name, trial,
                    "INCONCLUSIVE", "precondition",
                    "R1 did not observe version 1",
                    observed_1=v1,
                )
            )
            continue

        ok, row2, err = read_item(
            nodes[remote_name],
            run_id,
            "MR",
            config_name,
            trial,
            "x",
            read_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "MR", config_name, trial,
                    "FAILED", "read2", err,
                    observed_1=v1,
                )
            )
            continue

        v2 = version_of(row2)
        outcome = "BREACH" if v2 is None or v2 < v1 else "PASS"

        out.append(
            result_row(
                scenario, run_id, "MR", config_name, trial,
                outcome,
                "check",
                f"R1 via n1, R2 via {remote_name}",
                observed_1=v1,
                observed_2=v2,
            )
        )

    return out


def test_monotonic_writes(
    nodes,
    scenario,
    run_id,
    config_name,
    write_cl,
    read_cl,
    trials,
    remote_name,
):
    """
    Operationalization of monotonic writes using prefix visibility.

    Same logical client issues sequential writes:
        W1(w1=1) completes
        -> W2(w2=1) completes

    Observer on the W2 side:
        if W2 is visible but W1 is not, the observer has seen a later
        client write without its earlier write => monotonic-writes breach.

    Distinct items are used so Cassandra's last-write-wins conflict
    resolution on one row does not confuse write ordering with timestamp
    conflict resolution.
    """
    out = []
    w1_node = nodes["n1"]
    w2_node = nodes[remote_name]
    observer = nodes[remote_name]

    for trial in range(trials):
        ok, err = write_item(
            w1_node,
            run_id,
            "MW",
            config_name,
            trial,
            "w1",
            1,
            "first-write",
            "client-A",
            write_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "MW", config_name, trial,
                    "FAILED", "write1", err
                )
            )
            continue

        # Strict program order: W2 is not issued until W1 has returned.
        ok, err = write_item(
            w2_node,
            run_id,
            "MW",
            config_name,
            trial,
            "w2",
            1,
            "second-write",
            "client-A",
            write_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "MW", config_name, trial,
                    "FAILED", "write2", err
                )
            )
            continue

        ok, row2, err = read_item(
            observer,
            run_id,
            "MW",
            config_name,
            trial,
            "w2",
            read_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "MW", config_name, trial,
                    "FAILED", "observe-w2", err
                )
            )
            continue

        v2 = version_of(row2)
        if v2 is None or v2 < 1:
            out.append(
                result_row(
                    scenario, run_id, "MW", config_name, trial,
                    "INCONCLUSIVE", "precondition",
                    "Observer did not see W2",
                    observed_1=v2,
                )
            )
            continue

        ok, row1, err = read_item(
            observer,
            run_id,
            "MW",
            config_name,
            trial,
            "w1",
            read_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "MW", config_name, trial,
                    "FAILED", "observe-w1", err,
                    observed_1=v2,
                )
            )
            continue

        v1 = version_of(row1)
        outcome = "BREACH" if v1 is None or v1 < 1 else "PASS"

        out.append(
            result_row(
                scenario, run_id, "MW", config_name, trial,
                outcome,
                "check",
                f"W1 via n1, W2+observer via {remote_name}",
                observed_1=v2,  # later write
                observed_2=v1,  # earlier write
            )
        )

    return out


def test_writes_follow_reads(
    nodes,
    scenario,
    run_id,
    config_name,
    write_cl,
    read_cl,
    trials,
    remote_name,
):
    """
    Writes-follow-reads (explicit causal dependency):

        Client A: W(cause=1)
        Client B: R(cause=1)
        Client B: W(effect=1)

    The dependency exists only if Client B actually read cause=1.

    Then an observer on the effect side checks:
        effect=1 visible AND cause<1
    which means the dependent write became visible without the write that
    the client had observed before issuing it.
    """
    out = []
    cause_side = nodes["n1"]
    effect_side = nodes[remote_name]

    for trial in range(trials):
        # W(cause=1)
        ok, err = write_item(
            cause_side,
            run_id,
            "WFR",
            config_name,
            trial,
            "cause",
            1,
            "cause-v1",
            "client-A",
            write_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "WFR", config_name, trial,
                    "FAILED", "cause-write", err
                )
            )
            continue

        # Client B reads the cause.
        ok, cause_read, err = read_item(
            cause_side,
            run_id,
            "WFR",
            config_name,
            trial,
            "cause",
            read_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "WFR", config_name, trial,
                    "FAILED", "dependency-read", err
                )
            )
            continue

        cause_seen = version_of(cause_read)
        if cause_seen is None or cause_seen < 1:
            out.append(
                result_row(
                    scenario, run_id, "WFR", config_name, trial,
                    "INCONCLUSIVE", "precondition",
                    "Client B did not observe cause=1, so no causal chain formed",
                    observed_1=cause_seen,
                )
            )
            continue

        # Because the read above has completed, this write is causally after it.
        ok, err = write_item(
            effect_side,
            run_id,
            "WFR",
            config_name,
            trial,
            "effect",
            1,
            "effect-after-cause",
            "client-B",
            write_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "WFR", config_name, trial,
                    "FAILED", "effect-write", err,
                    observed_1=cause_seen,
                )
            )
            continue

        # Observer first establishes that the dependent write is visible.
        ok, effect_row, err = read_item(
            effect_side,
            run_id,
            "WFR",
            config_name,
            trial,
            "effect",
            read_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "WFR", config_name, trial,
                    "FAILED", "observe-effect", err,
                    observed_1=cause_seen,
                )
            )
            continue

        effect_seen = version_of(effect_row)
        if effect_seen is None or effect_seen < 1:
            out.append(
                result_row(
                    scenario, run_id, "WFR", config_name, trial,
                    "INCONCLUSIVE", "precondition",
                    "Observer did not see the dependent effect write",
                    observed_1=effect_seen,
                )
            )
            continue

        # If effect is visible here, its causal predecessor should also be
        # visible for writes-follow-reads.
        ok, cause_row, err = read_item(
            effect_side,
            run_id,
            "WFR",
            config_name,
            trial,
            "cause",
            read_cl,
        )
        if not ok:
            out.append(
                result_row(
                    scenario, run_id, "WFR", config_name, trial,
                    "FAILED", "observe-cause", err,
                    observed_1=effect_seen,
                )
            )
            continue

        cause_visible_on_effect_side = version_of(cause_row)
        outcome = (
            "BREACH"
            if cause_visible_on_effect_side is None
            or cause_visible_on_effect_side < 1
            else "PASS"
        )

        out.append(
            result_row(
                scenario, run_id, "WFR", config_name, trial,
                outcome,
                "check",
                f"cause via n1; effect+observer via {remote_name}",
                observed_1=effect_seen,
                observed_2=cause_visible_on_effect_side,
            )
        )

    return out


def choose_remote_node(scenario):
    """
    normal:
        use n3 as the alternate coordinator.

    node-failure:
        assume n3 is the failed node, so test client movement among surviving
        n1/n2 nodes.

    partition:
        assume n3 is isolated from n1/n2 but remains reachable from the test
        client via its published host port. This is the most useful topology
        for exposing stale/causal anomalies at CL.ONE.
    """
    if scenario == "node-failure":
        return "n2"
    return "n3"


def print_node_identities(nodes):
    print("\nPinned coordinator endpoints:")
    for name, node in nodes.items():
        ident = node.identity()
        print(
            f"  {name}: 127.0.0.1:{node.port} "
            f"host_id={ident.host_id} dc={ident.data_center} rack={ident.rack}"
        )

    host_ids = [str(node.identity().host_id) for node in nodes.values()]
    if len(set(host_ids)) != 3:
        raise RuntimeError(
            "The three host-side ports did not resolve to three distinct "
            "Cassandra host_ids. Check Docker port mappings before running "
            "the experiment."
        )


def run_suite(args):
    configs = parse_configs(args.configs)
    run_id = args.run_id or uuid.uuid4().hex[:12]
    output_path = Path(args.output)

    nodes = {}
    all_results = []

    try:
        print("Connecting to Cassandra nodes...")
        for name, port in NODE_PORTS.items():
            nodes[name] = NodeClient(name, port, args.request_timeout)

        print_node_identities(nodes)
        setup_schema(nodes)
        seed_baselines(nodes, run_id, configs, args.trials)

        remote_name = choose_remote_node(args.scenario)

        if args.scenario != "normal":
            print("\n" + "=" * 72)
            if args.scenario == "node-failure":
                print(
                    "BASELINE READY. Inject the node failure now.\n"
                    "Expected setup: cass3 is unavailable; cass1 and cass2 survive.\n"
                    "Example: docker stop cass3"
                )
            else:
                print(
                    "BASELINE READY. Inject the network partition now.\n"
                    "Expected setup: cass3 cannot communicate with cass1/cass2,\n"
                    "but the Python client can still reach cass3 through port 9044.\n"
                    "Do NOT partition before this prompt, because baseline rows are\n"
                    "seeded at CL.ALL."
                )
            print("=" * 72)
            input("Press ENTER after the fault/partition is active... ")

        print(
            f"\nRunning scenario={args.scenario}, run_id={run_id}, "
            f"trials={args.trials}"
        )

        for w_name, r_name in configs:
            write_cl = CL_MAP[w_name]
            read_cl = CL_MAP[r_name]
            config_name = f"W={w_name},R={r_name}"

            print(f"\n--- {config_name} ---")

            model_results = []
            model_results += test_ryw(
                nodes, args.scenario, run_id, config_name,
                write_cl, read_cl, args.trials, remote_name
            )
            model_results += test_monotonic_reads(
                nodes, args.scenario, run_id, config_name,
                write_cl, read_cl, args.trials, remote_name
            )
            model_results += test_monotonic_writes(
                nodes, args.scenario, run_id, config_name,
                write_cl, read_cl, args.trials, remote_name
            )
            model_results += test_writes_follow_reads(
                nodes, args.scenario, run_id, config_name,
                write_cl, read_cl, args.trials, remote_name
            )

            all_results.extend(model_results)

            counts = Counter(r["outcome"] for r in model_results)
            print(
                "  "
                + " | ".join(
                    f"{k}={counts.get(k, 0)}"
                    for k in ("PASS", "BREACH", "FAILED", "INCONCLUSIVE")
                )
            )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "scenario",
            "run_id",
            "model",
            "config",
            "trial",
            "outcome",
            "stage",
            "detail",
            "observed_1",
            "observed_2",
        ]

        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)

        print("\n=== SUMMARY BY MODEL / CONFIG ===")
        grouped = defaultdict(Counter)
        for row in all_results:
            grouped[(row["model"], row["config"])][row["outcome"]] += 1

        for (model, config_name), counts in sorted(grouped.items()):
            print(
                f"{model:4s} {config_name:22s} "
                f"PASS={counts.get('PASS', 0):3d} "
                f"BREACH={counts.get('BREACH', 0):3d} "
                f"FAILED={counts.get('FAILED', 0):3d} "
                f"INCONCLUSIVE={counts.get('INCONCLUSIVE', 0):3d}"
            )

        print(f"\nCSV written to: {output_path.resolve()}")
        print(f"run_id: {run_id}")

    finally:
        for node in nodes.values():
            try:
                node.shutdown()
            except Exception:
                pass


def build_parser():
    default_configs = ",".join(
        f"{w}:{r}" for w, r in DEFAULT_CONFIGS
    )

    p = argparse.ArgumentParser(
        description="Cassandra client-centric consistency experiments"
    )
    p.add_argument(
        "--scenario",
        choices=["normal", "node-failure", "partition"],
        default="normal",
        help="Fault scenario to test",
    )
    p.add_argument(
        "--trials",
        type=int,
        default=30,
        help="Trials per model per consistency configuration",
    )
    p.add_argument(
        "--configs",
        default=default_configs,
        help=(
            "Comma-separated WRITE:READ consistency pairs. "
            f"Default: {default_configs}"
        ),
    )
    p.add_argument(
        "--output",
        default="results/consistency_results.csv",
        help="CSV output path",
    )
    p.add_argument(
        "--run-id",
        default=None,
        help="Optional run id; generated automatically if omitted",
    )
    p.add_argument(
        "--request-timeout",
        type=float,
        default=4.0,
        help="Driver request timeout in seconds",
    )
    return p


if __name__ == "__main__":
    parser = build_parser()
    run_suite(parser.parse_args())
