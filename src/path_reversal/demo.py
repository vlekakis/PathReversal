"""Interactive demo of the Path Reversal algorithm.

Runs an in-process simulation with Rich terminal visualization,
showing the algorithm step-by-step as nodes compete for a shared token.

Usage:
    python -m path_reversal.demo [--nodes N] [--speed SECONDS] [--scenario FILE]
"""

import argparse
import time
import pickle
from collections import deque

from rich.console import Console
from rich.live import Live

from path_reversal.algorithm import PathReversal
from path_reversal.status import PRStatus, PRNext
from path_reversal.message import MsgType
from path_reversal.visualization import AlgorithmVisualizer

import logging
logger = logging.getLogger(__name__)


class DemoSimulation:
    """In-process simulation with visualization hooks."""

    def __init__(self, n_nodes, holder_index=0, speed=0.8):
        self.names = [f"node:{5001 + i}" for i in range(n_nodes)]
        self.nodes = {}
        self.holder = self.names[holder_index]
        self.speed = speed
        self.pending = deque()

        # Initialize nodes
        for name in self.names:
            if name == self.holder:
                self.nodes[name] = PathReversal(self.holder, "TOKEN", logger)
            else:
                self.nodes[name] = PathReversal(self.holder, None, logger)

        # Set up visualizer
        self.viz = AlgorithmVisualizer(self.names)
        self.viz.set_initial_state(self.holder)

    def _friendly(self, name):
        return self.viz._friendly(name)

    def make_hungry(self, node_name):
        """Make a node hungry and enqueue its request."""
        node = self.nodes[node_name]
        result = node.becomeHungry(node_name)
        if result is False:
            return
        msg_bytes, dest = result
        msg = pickle.loads(msg_bytes)
        self.pending.append((dest, msg))

        self.viz.update_node(node_name, status=PRStatus.HUNGRY,
                             last=None,
                             event_text=f"{self._friendly(node_name)} becomes HUNGRY, "
                                        f"sends request to {self._friendly(dest)}")

    def step(self):
        """Process one message. Returns True if a message was processed."""
        if not self.pending:
            return False

        dest, msg = self.pending.popleft()
        node = self.nodes[dest]

        if msg[MsgType.TYPE] == MsgType.PR_REQ:
            action, action_arg = node.recv(dest, msg)
            requester = msg[MsgType.SOURCE]

            if action == PRNext.FORWARD:
                fwd_msg = {
                    MsgType.TYPE: MsgType.PR_REQ,
                    MsgType.DST: action_arg,
                    MsgType.SOURCE: requester,
                    MsgType.DATA: None,
                    MsgType.DATA_ID: None,
                }
                self.pending.append((action_arg, fwd_msg))
                self.viz.update_node(
                    dest, status=node.status,
                    last=node.last, next_node=node.next,
                    queue_size=len(node.request_queue),
                    event_text=f"{self._friendly(dest)} forwards request "
                               f"to {self._friendly(action_arg)} "
                               f"(edge reversed: last={self._friendly(node.last)})")

            elif action == PRNext.TX_OBJ:
                obj_msg = {
                    MsgType.TYPE: MsgType.PR_OBJ,
                    MsgType.DST: requester,
                    MsgType.SOURCE: dest,
                    MsgType.DATA: action_arg,
                    MsgType.DATA_ID: None,
                }
                self.pending.append((requester, obj_msg))
                self.viz.update_node(
                    dest, status=node.status,
                    last=node.last, next_node=node.next,
                    queue_size=len(node.request_queue),
                    event_text=f"{self._friendly(dest)} gives TOKEN "
                               f"to {self._friendly(requester)}, "
                               f"becomes THINKING")

            elif action == PRNext.QUEUED:
                self.viz.update_node(
                    dest, status=node.status,
                    last=node.last, next_node=node.next,
                    queue_size=len(node.request_queue),
                    event_text=f"{self._friendly(dest)} queues request from "
                               f"{self._friendly(requester)} (also hungry)")

        elif msg[MsgType.TYPE] == MsgType.PR_OBJ:
            node.becomeEating(msg[MsgType.DATA])
            self.viz.update_node(
                dest, status=PRStatus.EATING,
                last=node.last, next_node=node.next,
                queue_size=len(node.request_queue),
                event_text=f"{self._friendly(dest)} receives TOKEN, "
                           f"now EATING!")

            # Serve queued requests
            if node.request_queue:
                forward = node.becomeThinking()
                if forward is not None:
                    next_node, data = forward
                    obj_msg = {
                        MsgType.TYPE: MsgType.PR_OBJ,
                        MsgType.DST: next_node,
                        MsgType.SOURCE: dest,
                        MsgType.DATA: data,
                        MsgType.DATA_ID: None,
                    }
                    self.pending.append((next_node, obj_msg))
                    self.viz.update_node(
                        dest, status=node.status,
                        last=node.last, next_node=node.next,
                        queue_size=len(node.request_queue),
                        event_text=f"{self._friendly(dest)} done eating, "
                                   f"forwards TOKEN to queued "
                                   f"{self._friendly(next_node)}")

        return True

    def run_demo(self, scenario):
        """Run a demo scenario with live visualization."""
        console = Console()

        console.print("\n[bold cyan]Path Reversal Algorithm Demo[/bold cyan]")
        console.print("[dim]Press Ctrl+C to stop\n[/dim]")

        with Live(self.viz.render(), console=console, refresh_per_second=4) as live:
            time.sleep(self.speed * 2)  # Show initial state

            for action in scenario:
                if action[0] == 'hungry':
                    self.make_hungry(action[1])
                    live.update(self.viz.render())
                    time.sleep(self.speed)

                elif action[0] == 'sleep':
                    time.sleep(float(action[1]))

                elif action[0] == 'step':
                    # Process messages one at a time with visualization
                    while self.step():
                        live.update(self.viz.render())
                        time.sleep(self.speed)

            # Process remaining messages
            while self.step():
                live.update(self.viz.render())
                time.sleep(self.speed)

            self.viz.log_event("[bold green]Simulation complete![/bold green]")
            live.update(self.viz.render())
            time.sleep(self.speed * 3)


def parse_scenario_file(filepath):
    """Parse a scenario file into demo actions."""
    actions = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split()
            if parts[0] == 'set':
                # Initial setup handled in simulation init
                pass
            elif parts[0] == 'hungry':
                actions.append(('hungry', parts[1]))
                actions.append(('step', None))
            elif parts[0] == 'sleep':
                actions.append(('sleep', parts[1]))
            elif parts[0] == 'exit':
                break
    return actions


def default_scenario(n_nodes):
    """Generate a default demo scenario."""
    names = [f"node:{5001 + i}" for i in range(n_nodes)]
    actions = []

    # Make nodes hungry one at a time with steps between
    for i in range(1, n_nodes):
        actions.append(('hungry', names[i]))
        actions.append(('step', None))
        actions.append(('sleep', '0.5'))

    return actions


def concurrent_scenario(n_nodes):
    """All non-holder nodes become hungry simultaneously."""
    names = [f"node:{5001 + i}" for i in range(n_nodes)]
    actions = []

    # All nodes become hungry at once
    for i in range(1, n_nodes):
        actions.append(('hungry', names[i]))

    # Then process all messages
    actions.append(('step', None))

    return actions


def main():
    p = argparse.ArgumentParser(description='Path Reversal Algorithm Demo')
    p.add_argument('--nodes', type=int, default=5,
                   help='Number of nodes (default: 5)')
    p.add_argument('--speed', type=float, default=0.8,
                   help='Seconds between steps (default: 0.8)')
    p.add_argument('--scenario', default=None,
                   help='Scenario file (optional)')
    p.add_argument('--concurrent', action='store_true',
                   help='Use concurrent hungry scenario')
    args = p.parse_args()

    sim = DemoSimulation(args.nodes, holder_index=0, speed=args.speed)

    if args.scenario:
        scenario = parse_scenario_file(args.scenario)
    elif args.concurrent:
        scenario = concurrent_scenario(args.nodes)
    else:
        scenario = default_scenario(args.nodes)

    try:
        sim.run_demo(scenario)
    except KeyboardInterrupt:
        Console().print("\n[dim]Demo stopped.[/dim]")


if __name__ == "__main__":
    main()
