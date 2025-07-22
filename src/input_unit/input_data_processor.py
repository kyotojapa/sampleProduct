"""Data processor for a client that connects to the socket created by the
TCPListener. It reads messages sent by the client and puts them onto the
correct queue owned by the data store"""

import json
from socket import socket
from typing import List
from pydantic import ValidationError
import product_common_logging as logging

from config_manager.models import GeneralConfig
from data_processing.models.unified_model import UnifiedModel
from input_unit.input_data_filter import has_valid_object_id
from input_unit.input_data_store import InputDataStore
from input_unit.shared_data import (ClientDisconnectedException, EventType,
                                    MessageType, PurgeScheme, get_events_queue)

LF_STR = b"\n"
METRICS_IDENTIFY_KEYWORD = "snapshot"


class InputDataProcessor:
    """The DataProcessor reads messages sent on a socket by a client and
    processes them in order to pass them to the queues of the data store"""

    def __init__(
            self,
            active_sock: socket,
            input_data_store: InputDataStore,
            config: GeneralConfig,
    ):
        """Method to Initialise the shared object and data store from
        parameters"""

        self.event_queue = get_events_queue()
        self.active_sock = active_sock
        self.config = config
        self._input_data_store = input_data_store
        self.chunk_size = self.config.input_buffer_chunk_size
        self.buffer_size = self.config.input_buffer_size
        self.purge_scheme = self.config.input_buffer_purge_scheme
        self.input_buffer_error_scheme = self.config.input_buffer_error_scheme
        self.buffer = bytearray(self.buffer_size)
        self.temp_buffer = bytearray(self.chunk_size)

    def process_event(self):
        """Method called when there is read event in the listener to add data
        to buffer in chunks and process them when the buffer contains a new
        line"""

        try:
            self.temp_buffer = self.active_sock.recv(self.chunk_size)
        except (BlockingIOError, InterruptedError, OSError, TimeoutError) as err:
            logging.error(f"exception at process_event:  {err}")
            self.event_queue.put(EventType.CLIENT_TIMED_OUT_CONNECTION)
            raise ClientDisconnectedException(None) from err
        else:
            if self.temp_buffer:
                self.process_buffer()
            else:
                logging.info("Peer closed.")
                self.event_queue.put(EventType.CLIENT_CLOSED_CONNECTION)
                raise ClientDisconnectedException(self.active_sock)

    def process_buffer(self):
        """Method that process the temp buffer data"""
        lines: List[bytes] = []
        index = 0
        if self.temp_buffer:
            self.buffer.extend(self.temp_buffer)
            while True:
                pos = self.buffer.find(b"\n", index)
                if pos == -1:
                    break
                line = self.buffer[index:pos]
                lines.append(bytes(line))
                index = pos + 1
            if lines:
                self.process_lines(lines)
                self.buffer = self.buffer[index:]

    def get_message_type(self, message) -> MessageType:
        """Method that tries to extract the message type of the json entry"""

        if hasattr(message, "get"):
            if METRICS_IDENTIFY_KEYWORD in message:
                return MessageType.METRIC

            return MessageType.LOG

        return MessageType.UNKNOWN

    def process_lines(self, lines: List[bytes]):
        """Method extracting json messages. It assumes that the chunk
        size is small enough that there is only going to be one message"""

        for line in lines:
            decoded_line = self.decode_data(line)

            try:
                if decoded_line:
                    json_log = json.loads(decoded_line)
                    if json_log:
                        message_type = self.get_message_type(json_log)

                        if message_type != MessageType.UNKNOWN:
                            if message_type == MessageType.METRIC:
                                if has_valid_object_id(json_log):
                                    parsed_log = UnifiedModel.parse_obj(json_log)
                                    self.handle_json_parse_success(
                                        message_type, parsed_log)
                                else:
                                    logging.debug("Metric ingested and filtered out by obj id.")
                            else:
                                if self.purge_scheme == PurgeScheme.ENABLED:
                                    if len(decoded_line) <= self.buffer_size:
                                        self.handle_json_parse_success(
                                            message_type, json_log)
                                    else:
                                        self.buffer = self.buffer[
                                                      len(self.buffer):]
                                        logging.debug(
                                            "Big log ingested and dropped. "
                                            f"Length: {len(decoded_line)}")
                                else:
                                    self.handle_json_parse_success(
                                        message_type, json_log)
                    else:
                        logging.debug(
                            "Failed to extract json log. No data after loading.")

            except (json.JSONDecodeError, ValidationError) as parse_err:
                logging.debug(f"Json decode exception or validation error: "
                              f"{parse_err}")

    def handle_json_parse_success(self, message_type, parsed_log):
        """Method handle json parse success"""

        if message_type:
            self._input_data_store.put_message(message_type, parsed_log)
        else:
            logging.debug("Could not determine log type")

    def decode_data(self, line: bytes):
        """Method that processes data from the byte array containing a single
        log message and converts these to a string"""
        decoded_line = ""
        try:
            if line:
                decoded_line = line.decode(
                    encoding="utf-8",
                    errors=self.config.input_buffer_error_scheme.value.lower(),
                )

        except UnicodeDecodeError as uni_err:
            logging.error(
                f"Failed to decode line data - data is discarded {uni_err}")

        return decoded_line

    def __del__(self):
        """Cleanup opened sockets."""

        try:
            self.active_sock.close()
        except OSError:
            logging.error("Can not close client socket properly.")
