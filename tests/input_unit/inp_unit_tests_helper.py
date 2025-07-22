""" Module with helper methods to run tests on input unit"""

import socket
import os.path
import random
import selectors
import product_common_logging as logging  # type: ignore


def connect(address, port):
    """Method used to connect to the server"""

    # Set up the socket to use in order to send data
    server_address = (address, port)
    logging.info(f"Connecting to {address}:{port}")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(server_address)
    sock.setblocking(False)

    # Set up the selector to watch for when the socket is ready
    # to send data as well as when there is data to read.
    mysel = selectors.DefaultSelector()
    mysel.register(
        sock,
        selectors.EVENT_WRITE,
    )
    return sock, mysel


def close(sock, mysel):
    """Method to clean up"""

    try:
        mysel.unregister(sock)
        mysel.close()

    except OSError as os_e:
        logging.error(f"Exception cleaning up selector instance {os_e}")

    try:
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()

    except OSError as os_se:
        logging.error(f"Exception cleaning up socket instance {os_se}")


def send_file_data(sock, counter, file_name):
    """Sending the data in file"""

    keep_reading = True
    try:
        if os.path.isfile(file_name):
            with open(file_name,
                      mode='rt',
                      encoding='utf-8') as f_p:
                while keep_reading:
                    number_of_bytes_to_read = random.randint(500, 1400)
                    chunk = f_p.read(number_of_bytes_to_read)
                    if chunk == '':
                        keep_reading = False
                    else:
                        sock.sendall(bytearray(chunk, 'utf-8'))
                f_p.close()
    except BlockingIOError as err:
        print(f"Had exception cancelling run {counter}. Exception: "
              f"{err}")
        raise ConnectionRefusedError from err
