"""Unit tests for the Message module."""

import pickle
import pytest
from path_reversal.message import MsgType, MsgFactory


class TestMsgFactory:
    """Test message creation and serialization."""

    def test_create_message_roundtrip(self):
        """Created message should survive pickle roundtrip."""
        msg_bytes = MsgFactory.create(MsgType.DATA_MSG, dst="node1",
                                       data="hello", dataId="id123",
                                       src="node2")
        msg = pickle.loads(msg_bytes)
        assert msg[MsgType.TYPE] == MsgType.DATA_MSG
        assert msg[MsgType.DST] == "node1"
        assert msg[MsgType.DATA] == "hello"
        assert msg[MsgType.DATA_ID] == "id123"
        assert msg[MsgType.SOURCE] == "node2"

    def test_create_message_defaults_to_none(self):
        """Optional fields should default to None."""
        msg_bytes = MsgFactory.create(MsgType.AGENT_TEST_MSG)
        msg = pickle.loads(msg_bytes)
        assert msg[MsgType.TYPE] == MsgType.AGENT_TEST_MSG
        assert msg[MsgType.DST] is None
        assert msg[MsgType.DATA] is None
        assert msg[MsgType.DATA_ID] is None
        assert msg[MsgType.SOURCE] is None

    def test_create_returns_bytes(self):
        """create() should return bytes (pickled)."""
        result = MsgFactory.create(MsgType.KEEP_ALIVE)
        assert isinstance(result, bytes)


class TestMessageId:
    """Test message ID generation."""

    def test_deterministic_id(self):
        """Same data should produce same ID."""
        id1 = MsgFactory.generateMessageId("test_data")
        id2 = MsgFactory.generateMessageId("test_data")
        assert id1 == id2

    def test_unique_ids(self):
        """Different data should produce different IDs."""
        id1 = MsgFactory.generateMessageId("data_a")
        id2 = MsgFactory.generateMessageId("data_b")
        assert id1 != id2

    def test_id_is_hex_string(self):
        """ID should be a hex digest string."""
        msg_id = MsgFactory.generateMessageId("anything")
        assert isinstance(msg_id, str)
        assert len(msg_id) == 32  # MD5 hex digest length
        int(msg_id, 16)  # Should not raise


class TestMsgTypeConstants:
    """Verify message type constants exist and are unique."""

    def test_all_types_are_integers(self):
        """All message type constants should be integers."""
        types = [
            MsgType.DATA_MSG, MsgType.DATA_ACK, MsgType.NEIGHBOR_ADD,
            MsgType.NEIGHBOR_RMV, MsgType.AGENT_TEST_MSG, MsgType.AGENT_TEST_ACK,
            MsgType.KEEP_ALIVE, MsgType.KEEP_ALIVE_ACK, MsgType.DATA_MOVE_MSG,
            MsgType.DATA_NACK, MsgType.FIFO_STATS_QUERY,
            MsgType.PR_REQ, MsgType.PR_OBJ, MsgType.PR_SETUP,
            MsgType.PR_GET_HUNGRY, MsgType.PR_REQ_ACK, MsgType.PR_OBJ_ACK,
            MsgType.PR_ACK, MsgType.PR_STATUS_UPDATE, MsgType.PR_STATUS_ACK,
        ]
        for t in types:
            assert isinstance(t, int)

    def test_pr_types_are_unique(self):
        """Path reversal message types should have unique values."""
        pr_types = [
            MsgType.PR_REQ, MsgType.PR_OBJ, MsgType.PR_SETUP,
            MsgType.PR_GET_HUNGRY, MsgType.PR_ACK,
            MsgType.PR_STATUS_UPDATE, MsgType.PR_STATUS_ACK,
        ]
        assert len(pr_types) == len(set(pr_types))
