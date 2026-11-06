# Distributed Power Outage & Load-Shedding Monitor

A distributed systems prototype that monitors power outages and load-shedding across multiple regions.

## Features

- Multiple independent regional nodes (A, B, C)
- Persistent outbox pattern (devices keep sending even when network is down)
- Hybrid Logical Clocks for event ordering
- Anti-entropy synchronization between nodes
- Quorum-based cell status (OK / DEGRADED / OUT / UNREACHABLE)
- Last-gasp messages from battery-powered devices
- Live web dashboard with failure injection controls
- Fully works offline (stdlib only — no external libraries)

## How to Run

```bash
python monitor.py