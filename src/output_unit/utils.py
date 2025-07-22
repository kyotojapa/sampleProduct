"""Utils module used for storing shareable data between
Output Unit, Policies and Adapters.
"""
from enum import Enum, auto


class BufferNotFoundError(Exception):
    """Custom Exception class for invalid BufferName.
    """
    def __init__(self, message: str) -> None:
        super().__init__(message)


class BufferFullError(Exception):
    """Custom Exception class for Output Buffer full.
    """
    def __init__(self, message: str) -> None:
        super().__init__(message)


class InvalidDataFormatError(Exception):
    """Custom Exception class for invalid Data type that might be enqueued.
    """
    def __init__(self, message: str) -> None:
        super().__init__(message)


class BufferNames(str, Enum):
    """Enum used for storing the names of the output buffers,
    Logs output buffer and Metrics output buffer.
    """
    LOGS_BUFFER = auto()
    METRICS_BUFFER = auto()
