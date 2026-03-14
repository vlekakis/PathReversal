# Path Reversal

Distributed mutual exclusion using the Path Reversal algorithm. Nodes in a ring compete for a shared token using directed pointer reversal to guarantee exactly one node holds the token at any time.

## Quick Start

```bash
pip install -e ".[dev]"

# Run the interactive demo
python -m path_reversal

# Run tests
pytest tests/ -v
```

## Demo

The Rich terminal demo visualizes the algorithm step-by-step:

```bash
# Default: 5 nodes, sequential hungry requests
python -m path_reversal

# Custom node count and speed
python -m path_reversal --nodes 6 --speed 0.5

# All nodes hungry simultaneously
python -m path_reversal --nodes 4 --concurrent

# Use a scenario file
python -m path_reversal --scenario src/util/example_scenario.txt
```

## How It Works

Nodes form a directed graph where `last` pointers point toward the token holder.

| State | Meaning |
|-------|---------|
| **THINKING** | Idle. `last` points toward the token. |
| **HUNGRY** | Wants the token. Sends request along `last`, clears it. |
| **EATING** | Has the token (in critical section). |

When a request passes through a node, the edge **reverses** (`last = requester`), creating a path back to the requester. When the token holder receives a request, it gives up the token and reverses its pointers.

**Key invariant:** Exactly one node is EATING at any time.

## ZMQ Network Architecture

For the full distributed simulation (not the in-process demo):

```bash
python -m path_reversal.driver -n src/util/node.txt -s src/util/example_scenario.txt -c -v
```

Nodes communicate via ZMQ in a ring topology:
- **PUB/SUB**: Driver broadcasts commands to all nodes
- **REQ/REP**: Peer-to-peer ring communication and driver status reporting

## Project Structure

```
src/path_reversal/
  algorithm.py      # Core PathReversal class (recv, becomeHungry, becomeThinking)
  message.py        # Message types and factory
  status.py         # PRStatus, PRNext, NodeStatus enums
  node.py           # FifoNode — ZMQ node process
  sink.py           # AgentSinkServer — driver/node bridge
  driver.py         # Driver — orchestrator and verifier
  visualization.py  # Rich terminal UI
  demo.py           # In-process demo runner
tests/
  unit/             # Algorithm and message unit tests
  integration/      # Multi-node simulation tests
```

## Tests

```bash
# All tests (41 total)
pytest tests/ -v

# Unit tests only (algorithm + message)
pytest tests/unit/ -v

# Integration tests (multi-node simulation)
pytest tests/integration/ -v
```
