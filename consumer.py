import socket

import pyarrow as pa


def main():
    host = "producer"  # The hostname of the producer container
    port = 12345

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        print("Connected")

        data = client_socket.recv(4 * 1024)
        with pa.ipc.open_stream(data) as reader:
            schema = reader.schema
            batches = [b for b in reader]
            print("schema:")
            print(schema)
            print("batches:")
            print(batches)


if __name__ == "__main__":
    main()
