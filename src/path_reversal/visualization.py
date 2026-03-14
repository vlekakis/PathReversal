"""Rich terminal visualization for the Path Reversal algorithm.

Displays a live-updating table of node states and an event log,
providing a clear view of the algorithm in action.
"""

import time
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from collections import deque

from path_reversal.status import PRStatus, PRNext

STATUS_STYLE = {
    PRStatus.EATING: ("EATING", "bold green"),
    PRStatus.HUNGRY: ("HUNGRY", "bold yellow"),
    PRStatus.THINKING: ("THINKING", "dim"),
}


class AlgorithmVisualizer:
    """Rich-based terminal visualization of Path Reversal algorithm state."""

    def __init__(self, node_names, max_events=20):
        self.console = Console()
        self.node_names = node_names
        self.node_states = {}
        self.events = deque(maxlen=max_events)
        self.step = 0
        self.eating_history = []

        # Initialize all nodes as unknown
        for name in node_names:
            self.node_states[name] = {
                'status': None,
                'last': None,
                'next': None,
                'queue': 0,
                'friendly': f"Node{node_names.index(name) + 1:03d}",
            }

    def set_initial_state(self, holder):
        """Set initial state: one node eating, others thinking."""
        for name in self.node_names:
            if name == holder:
                self.node_states[name]['status'] = PRStatus.EATING
                self.node_states[name]['last'] = None
            else:
                self.node_states[name]['status'] = PRStatus.THINKING
                self.node_states[name]['last'] = holder
            self.node_states[name]['next'] = None
            self.node_states[name]['queue'] = 0

        self.events.append(("INIT", f"Token placed at {self._friendly(holder)}"))

    def _friendly(self, name):
        """Get friendly name for a node."""
        if name is None:
            return "-"
        state = self.node_states.get(name)
        if state:
            return state['friendly']
        return str(name)

    def update_node(self, name, status=None, last=None, next_node=None,
                    queue_size=0, event_text=None):
        """Update a node's displayed state."""
        if name in self.node_states:
            if status is not None:
                self.node_states[name]['status'] = status
            if last is not None:
                self.node_states[name]['last'] = last
            self.node_states[name]['next'] = next_node
            self.node_states[name]['queue'] = queue_size

        if event_text:
            self.step += 1
            self.events.append((f"#{self.step:03d}", event_text))

    def log_event(self, text):
        """Add an event to the log."""
        self.step += 1
        self.events.append((f"#{self.step:03d}", text))

    def build_table(self):
        """Build the node status table."""
        table = Table(title="Path Reversal - Node Status",
                      title_style="bold cyan",
                      border_style="blue")
        table.add_column("Node", style="bold", width=10)
        table.add_column("Status", width=12, justify="center")
        table.add_column("Last", width=12, justify="center")
        table.add_column("Next", width=12, justify="center")
        table.add_column("Queue", width=8, justify="center")

        for name in self.node_names:
            state = self.node_states[name]
            friendly = state['friendly']

            # Status with color
            if state['status'] is not None and state['status'] in STATUS_STYLE:
                label, style = STATUS_STYLE[state['status']]
                status_text = Text(label, style=style)
            else:
                status_text = Text("???", style="dim red")

            # Last pointer
            last_text = self._friendly(state.get('last'))

            # Next pointer
            next_text = self._friendly(state.get('next'))

            # Queue size
            q = state.get('queue', 0)
            queue_text = str(q) if q > 0 else "-"

            table.add_row(friendly, status_text, last_text, next_text, queue_text)

        return table

    def build_graph(self):
        """Build ASCII representation of the 'last' pointer graph."""
        lines = []
        for name in self.node_names:
            state = self.node_states[name]
            friendly = state['friendly']
            last = state.get('last')
            if last is not None:
                last_friendly = self._friendly(last)
                lines.append(f"  {friendly} --last--> {last_friendly}")
            else:
                status = state.get('status')
                if status == PRStatus.EATING:
                    lines.append(f"  {friendly} [TOKEN HOLDER]")
                else:
                    lines.append(f"  {friendly} --last--> (none)")

        return "\n".join(lines) if lines else "  (no pointers)"

    def build_event_log(self):
        """Build the event log panel."""
        if not self.events:
            return "  (no events yet)"
        lines = []
        for step, text in self.events:
            lines.append(f"  [{step}] {text}")
        return "\n".join(lines)

    def render(self):
        """Build the full display."""
        table = self.build_table()
        graph_text = self.build_graph()
        event_text = self.build_event_log()

        graph_panel = Panel(graph_text, title="Pointer Graph (last)",
                           border_style="green", padding=(0, 1))
        event_panel = Panel(event_text, title="Event Log",
                           border_style="yellow", padding=(0, 1))

        layout = Layout()
        layout.split_column(
            Layout(table, name="table", ratio=2),
            Layout(name="bottom", ratio=3),
        )
        layout["bottom"].split_row(
            Layout(graph_panel, name="graph"),
            Layout(event_panel, name="events"),
        )
        return layout
