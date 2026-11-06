# Design Document – Distributed Power Outage Monitor

## 1. Problem
Power outages and load-shedding are frequent and unpredictable. There is no shared, real-time view of which areas are affected, especially when network connectivity is also poor.

## 2. System Goals
- Keep accepting reports even when the network is partitioned
- Eventually make all regional nodes agree on the current state
- Distinguish between real power loss and network failure
- Remain simple enough to implement and defend in a short time

## 3. Key Design Decisions

### 3.1 Availability over Consistency (AP System)
We chose the AP side of the CAP theorem.  
When a network partition happens, nodes continue accepting and serving data locally instead of becoming unavailable.

### 3.2 Persistent Outbox Pattern
Devices write events to a local SQLite outbox first.  
They only delete the event after the regional node acknowledges it.  
This gives at-least-once delivery and works even if the node is temporarily unreachable.

### 3.3 Hybrid Logical Clocks (HLC)
Every event carries an HLC timestamp.  
This gives a total order of events even when physical clocks are skewed and nodes are partitioned.

### 3.4 Idempotent Ingest
Every event has a UUID.  
Nodes use `INSERT OR IGNORE`, so duplicate deliveries are safe.

### 3.5 Anti-Entropy Synchronization
Nodes periodically pull new events from their peers using a sequence-number cursor.  
This is a simple and reliable way to achieve eventual consistency.

### 3.6 Derived State (Event Sourcing style)
We do not store the current status of devices permanently.  
The current OK / SUSPECT / OUT status is calculated from the event log every time.  
This makes recovery and synchronization much simpler.

### 3.7 Quorum + Last-Gasp
- Devices with battery can send a final “POWER_LOST” message
- A cell is marked OUT only when a quorum of its devices report problems
- If a whole region goes silent with no last-gasp, it is marked UNREACHABLE

## 4. Failure Handling

| Failure Type              | How the system handles it                          |
|---------------------------|----------------------------------------------------|
| Device loses power        | Last-gasp message + quorum                         |
| Device loses network      | Events stay in outbox and are sent later           |
| Node is partitioned       | Node keeps serving locally, syncs when healed      |
| Node crashes              | SQLite + WAL survives, catches up on restart       |

## 5. What I would improve with more time
- Real geographic map (Leaflet)
- Stronger failure detector (φ-accrual)
- Authentication and access control
- Metrics and Prometheus export
- CRDTs for concurrent updates