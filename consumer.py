import socket

import pyarrow as pa


def main():
    host = "producer"  # The hostname of the producer container
    port = 12345

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        print("Connected")
        source = client_socket.recv(1000 * 1024**2)

    with pa.ipc.open_stream(source) as reader:
        for i, batch in enumerate(reader):
            print(f"batch[{i}] ({batch.get_total_buffer_size()} bytes):")
            print(batch)


if __name__ == "__main__":
    main()
