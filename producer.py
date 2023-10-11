import os
import socket

import numpy as np
import pyarrow as pa

import addressing


def main():
    if os.path.exists(addressing.handle):
        os.remove(addressing.handle)

    num_points = 328 * 225
    x = np.random.uniform(0, 1000, num_points).astype(np.float32)
    y = np.random.uniform(0, 1000, num_points).astype(np.float32)
    z = np.random.uniform(0, 1000, num_points).astype(np.float32)
    values = np.random.randint(0, 256, num_points, dtype=np.uint8)

    batch = pa.record_batch(
        [x, y, z, values],
        names=["x", "y", "z", "values"],
    )

    # Set up a socket server for IPC communication
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server_socket:
        server_socket.bind(addressing.handle)
        server_socket.listen()

        print(f"Listening for clients on {addressing.handle}")

        # Accept incoming connections
        connection, client_address = server_socket.accept()

        # Send the Arrow Array through IPC
        sink = connection.makefile("wb", int(1.10 * batch.get_total_buffer_size()))
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        connection.close()

        print(f"IPC data sent to consumer at {client_address}")


if __name__ == "__main__":
    main()
