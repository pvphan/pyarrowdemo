import socket

import numpy as np
import pyarrow as pa


def main():
    num_points = 500
    x = np.random.uniform(0, 1000, num_points).astype(np.float32)
    y = np.random.uniform(0, 1000, num_points).astype(np.float32)
    z = np.random.uniform(0, 1000, num_points).astype(np.float32)
    values = np.random.randint(0, 256, num_points, dtype=np.uint8)

    batch = pa.record_batch(
        [x, y, z, values],
        names=["x", "y", "z", "values"],
    )

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
        sink = sock.makefile("wb", int(1.10 * batch.get_total_buffer_size()))
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        print(f"IPC data sent to consumer at {addr}")


if __name__ == "__main__":
    main()
