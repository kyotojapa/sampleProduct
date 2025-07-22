""" Class that holds data for the TCP socket shared between
the data processor and the tcp listener"""

from enum import Enum, auto
from queue import Queue

MIN_PORT = 1024
MAX_PORT = 65535
DEFAULT_IP_ADDRESS = "0.0.0.0"
__EVENT_QUEUE: Queue = Queue()


class ClientDisconnectedException(Exception):
    """Exception for announcing that a client dropped connection."""

    def __init__(self, socket, *args: object) -> None:
        self.socket = socket
        super().__init__(*args)


class EventType(Enum):
    "Event for tcp connection"
    CLIENT_CLOSED_CONNECTION = auto()
    CLIENT_TIMED_OUT_CONNECTION = auto()
    ACCEPTED_CLIENT_CONNECTION = auto()


def get_events_queue():
    "Method to access event queue"

    return __EVENT_QUEUE


class MessageType(Enum):
    "Class that models to two metric types"
    LOG = auto()
    METRIC = auto()
    UNKNOWN = auto()


class PurgeScheme(str, Enum):
    """An Enumeration class for the buffer purge scheme"""

    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class DecoderErrorScheme(str, Enum):
    """An Enumeration class for the decoder error recovery scheme"""

    STRICT = "STRICT"
    REPLACE = "REPLACE"
    IGNORE = "IGNORE"
