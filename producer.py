import socket

import pyarrow as pa


def main():
    data = [
        pa.array([1, 2, 3, 4, 5]),
        pa.array(['foo', 'bar', 'baz', None, 'nice']),
        pa.array([True, None, False, True, None])
    ]

    batch = pa.record_batch(data, names=['f0', 'f1', 'f2'])

    host = "0.0.0.0"  # Listen on all available network interfaces
    port = 12345

    # Set up a socket server for IPC communication
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()

        print(f"Listening for clients on {host}:{port}")

        # Accept incoming connections
        sock, addr = server_socket.accept()

        # Send the Arrow Array through IPC
        sink = sock.makefile("wb", 65536)
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        print(f"IPC data sent to consumer at {addr}")


if __name__ == "__main__":
    main()
