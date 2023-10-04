# consumer.py
import time
import pyarrow as pa
import socket

def main():

    host = "producer"  # The hostname of the producer container
    port = 12345

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        print("Connected")

        data = client_socket.recv(1024)
        print(len(data))
        with pa.ipc.open_stream(data) as reader:
            schema = reader.schema
            batches = [b for b in reader]
            print(schema)
            print(batches)


if __name__ == "__main__":
    main()
