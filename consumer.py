import time
import socket

import pyarrow as pa
import addressing


def main():
    max_buffer_size = 512 * 1024 ** 2
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
        client_socket.connect(addressing.handle)
        print(f"Connected to UNIX socket: {addressing.handle}")
        source = b""
        source_len = len(source)
        num_tries = 5
        for i in range(num_tries):
            source += client_socket.recv(max_buffer_size)
            if source_len == len(source):
                break
            source_len = len(source)
            print(f"Recv: {len(source)} bytes, {time.time()}")

    with pa.ipc.open_stream(source) as reader:
        for i, batch in enumerate(reader):
            print(f"batch[{i}] ({batch.get_total_buffer_size()} bytes):")
            print(batch)


if __name__ == "__main__":
    main()
