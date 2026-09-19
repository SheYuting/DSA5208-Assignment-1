import threading
import time
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import RoundRobinPolicy
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout, WriteTimeout, OperationTimedOut
from cassandra.query import SimpleStatement
import subprocess


def get_cl_name(cl_value):
    return ConsistencyLevel.value_to_name.get(cl_value, str(cl_value))

def get_session(port):
    cluster = Cluster(['127.0.0.1'], port=port, load_balancing_policy=RoundRobinPolicy())
    return cluster.connect(), cluster

def setup_database(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS demo
        WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};
    """)
    session.set_keyspace('demo')
    session.execute("DROP TABLE IF EXISTS demo.events;")
    session.execute("CREATE TABLE demo.events (id int PRIMARY KEY, value int);")

# =========================================================
# 1. READ YOUR WRITES
# =========================================================
def test_read_your_writes(s1, s2, cl, trials=30):
    breaches, failures = 0, 0
    for i in range(trials):
        key, val = 100 + i, 999 + i
        barrier = threading.Barrier(2)
        res = {}

        def writer():
            barrier.wait()
            try:
                s1.execute(SimpleStatement(f"INSERT INTO events (id, value) VALUES ({key}, {val})", consistency_level=cl))
            except Exception:
                pass

        def reader():
            barrier.wait()
            time.sleep(0.0005)
            try:
                row = s2.execute(SimpleStatement(f"SELECT value FROM events WHERE id = {key}", consistency_level=cl)).one()
                res['val'] = row.value if row else None
            except (Unavailable, ReadTimeout, WriteTimeout, OperationTimedOut, NoHostAvailable):
                res['failed'] = True

        t1, t2 = threading.Thread(target=writer), threading.Thread(target=reader)
        t1.start(); t2.start()
        t1.join(); t2.join()

        if res.get('failed'):
            failures += 1
        elif res.get('val') != val:
            breaches += 1

    print(f"Read Your Writes ({get_cl_name(cl)}): {breaches} Breaches | {failures} Prevented/Failed")

# =========================================================
# 2. MONOTONIC READS (Targets Surviving Nodes s1 -> s2)
# =========================================================
def test_monotonic_reads(s1, s2, s3, cl, trials=30):
    breaches, failures = 0, 0
    for i in range(trials):
        key, val = 200 + i, 888 + i
        barrier = threading.Barrier(2)
        res = {}

        def writer_and_reader1():
            try:
                s1.execute(SimpleStatement(f"INSERT INTO events (id, value) VALUES ({key}, {val})", consistency_level=cl))
                row1 = s1.execute(SimpleStatement(f"SELECT value FROM events WHERE id = {key}", consistency_level=cl)).one()
                res['r1'] = row1.value if row1 else None
            except Exception:
                res['failed'] = True
            barrier.wait()

        def reader2():
            barrier.wait()
            try:
                # Read from lagging Node 2 (s2) instead of dead Node 3 (s3)
                row2 = s2.execute(SimpleStatement(f"SELECT value FROM events WHERE id = {key}", consistency_level=cl)).one()
                res['r2'] = row2.value if row2 else None
            except (Unavailable, ReadTimeout, WriteTimeout, OperationTimedOut, NoHostAvailable):
                res['failed'] = True

        t1, t2 = threading.Thread(target=writer_and_reader1), threading.Thread(target=reader2)
        t1.start(); t2.start()
        t1.join(); t2.join()

        if res.get('failed'):
            failures += 1
        elif res.get('r1') == val and res.get('r2') != val:
            breaches += 1  # Time went backward! Saw update on s1, saw stale/none on s2

    print(f"Monotonic Reads ({get_cl_name(cl)}): {breaches} Breaches | {failures} Prevented/Failed")

# =========================================================
# 3. WRITES FOLLOW READS (Targets Surviving Nodes s2 -> s1)
# =========================================================
def test_writes_follow_reads(s1, s2, s3, cl, trials=30):
    breaches, failures = 0, 0
    for i in range(trials):
        key = 300 + i
        v_initial, v_causal = 10, 20
        barrier = threading.Barrier(2)
        res = {}

        try:
            s1.execute(SimpleStatement(f"INSERT INTO events (id, value) VALUES ({key}, {v_initial})", consistency_level=ConsistencyLevel.ONE))
        except Exception:
            pass

        def client_a_update():
            try:
                s1.execute(SimpleStatement(f"INSERT INTO events (id, value) VALUES ({key}, {v_causal})", consistency_level=cl))
            except Exception:
                pass
            barrier.wait()

        def client_b_causal_write():
            barrier.wait()
            try:
                # Client B reads state from lagging Node 2 (s2)
                row = s2.execute(SimpleStatement(f"SELECT value FROM events WHERE id = {key}", consistency_level=cl)).one()
                # If Client B sees stale initial value (10), write causal payload 999 to Node 1 (s1)
                if row and row.value == v_initial:
                    s1.execute(SimpleStatement(f"INSERT INTO events (id, value) VALUES ({key}, 999)", consistency_level=cl))
            except (Unavailable, ReadTimeout, WriteTimeout, OperationTimedOut, NoHostAvailable):
                res['failed'] = True

        t1, t2 = threading.Thread(target=client_a_update), threading.Thread(target=client_b_causal_write)
        t1.start(); t2.start()
        t1.join(); t2.join()

        if res.get('failed'):
            failures += 1
        else:
            try:
                final = s1.execute(SimpleStatement(f"SELECT value FROM events WHERE id = {key}", consistency_level=ConsistencyLevel.ONE)).one()
                if final and final.value == 999:
                    breaches += 1  # Causal write executed based on stale read state!
            except Exception:
                pass

    print(f"Writes Follow Reads ({get_cl_name(cl)}): {breaches} Breaches | {failures} Prevented/Failed")

# =========================================================
# 4. MONOTONIC WRITES (Targets Surviving Nodes s1 & s2)
# =========================================================
def test_monotonic_writes(s1, s2, s3, cl, trials=30):
    breaches, failures = 0, 0
    for i in range(trials):
        key = 400 + i
        v1, v2 = 500, 600
        barrier = threading.Barrier(2)
        res = {}

        def write1():
            barrier.wait()
            try:
                s1.execute(SimpleStatement(f"INSERT INTO events (id, value) VALUES ({key}, {v1})", consistency_level=cl))
            except Exception:
                pass

        def write2():
            barrier.wait()
            time.sleep(0.0002)
            try:
                s2.execute(SimpleStatement(f"INSERT INTO events (id, value) VALUES ({key}, {v2})", consistency_level=cl))
            except Exception:
                pass

        t1, t2 = threading.Thread(target=write1), threading.Thread(target=write2)
        t1.start(); t2.start()
        t1.join(); t2.join()

        try:
            # Check final observer state on Node 1 (s1) instead of dead Node 3 (s3)
            final = s1.execute(SimpleStatement(f"SELECT value FROM events WHERE id = {key}", consistency_level=ConsistencyLevel.ONE)).one()
            if final and final.value == v1:
                breaches += 1  # Older write (v1) won over newer write (v2) due to ordering delay
        except (Unavailable, ReadTimeout, WriteTimeout, OperationTimedOut, NoHostAvailable):
            failures += 1

    print(f"Monotonic Writes ({get_cl_name(cl)}): {breaches} Breaches | {failures} Prevented/Failed")




# =========================================================
# MAIN DRIVER
# =========================================================
if __name__ == "__main__":
    s1, c1 = get_session(9042) # cass1
    s2, c2 = get_session(9043) # cass2
    s3, c3 = get_session(9044) # cass3

    setup_database(s1)
    s2.set_keyspace('demo')
    s3.set_keyspace('demo')

    print("\n" + "="*60)
    print("✅ SESSIONS CONNECTED TO ALL 3 NODES (9042, 9043, 9044).")
    print("If testing 'Node Off' (docker pause cass3) or 'Partition'")
    print("(docker network disconnect ... cass3), trigger it NOW in Admin Window.")
    input("Press ENTER to run tests...")
    print("="*60)

    print("\n=== EVENTUAL CONSISTENCY (CL.ONE) ===")
    test_read_your_writes(s1, s2, ConsistencyLevel.ONE)
    test_monotonic_reads(s1, s2, s3, ConsistencyLevel.ONE)
    test_writes_follow_reads(s1, s2, s3, ConsistencyLevel.ONE)
    test_monotonic_writes(s1, s2, s3, ConsistencyLevel.ONE)

    print("\n=== STRONG CONSISTENCY (CL.QUORUM) ===")
    test_read_your_writes(s1, s2, ConsistencyLevel.QUORUM)
    test_monotonic_reads(s1, s2, s3, ConsistencyLevel.QUORUM)
    test_writes_follow_reads(s1, s2, s3, ConsistencyLevel.QUORUM)
    test_monotonic_writes(s1, s2, s3, ConsistencyLevel.QUORUM)

    c1.shutdown(); c2.shutdown(); c3.shutdown()