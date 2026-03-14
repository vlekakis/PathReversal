"""Status enums for the Path Reversal system."""

from enum import IntEnum


class PRStatus(IntEnum):
    THINKING = 0
    HUNGRY = 1
    EATING = 2


class PRNext(IntEnum):
    FORWARD = 3
    RX_OBJ = 4
    TX_OBJ = 5
    QUEUED = 6


class NodeStatus(IntEnum):
    DRIVER_PARSED = 1
    DRIVER_INITIALIZED = 2
    DRIVER_FUNCTIONAL = 3
