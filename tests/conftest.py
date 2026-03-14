"""Shared test fixtures for Path Reversal tests."""

import logging
import pytest
from path_reversal.algorithm import PathReversal
from path_reversal.status import PRStatus
from path_reversal.message import MsgType


@pytest.fixture
def mock_logger():
    """A logger that captures debug output for assertions."""
    return logging.getLogger("test")


@pytest.fixture
def make_node(mock_logger):
    """Factory fixture to create PathReversal nodes."""
    def _make(item_holder=None, item=None):
        return PathReversal(item_holder, item, mock_logger)
    return _make


@pytest.fixture
def ring_of_nodes(mock_logger):
    """Create a ring of N nodes with one token holder.

    Returns (nodes_dict, node_names) where nodes_dict maps
    name -> PathReversal instance.
    """
    def _make(n=4, holder_index=0):
        names = [f"node:{5001 + i}" for i in range(n)]
        holder = names[holder_index]
        nodes = {}
        for name in names:
            if name == holder:
                nodes[name] = PathReversal(holder, "TOKEN", mock_logger)
            else:
                nodes[name] = PathReversal(holder, None, mock_logger)
        return nodes, names
    return _make


def make_request_msg(source):
    """Helper to create a PR_REQ-like message dict for recv()."""
    return {
        MsgType.TYPE: MsgType.PR_REQ,
        MsgType.DST: None,  # Set by caller
        MsgType.SOURCE: source,
        MsgType.DATA: None,
        MsgType.DATA_ID: None,
    }
